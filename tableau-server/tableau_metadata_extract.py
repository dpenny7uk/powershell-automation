"""
Tableau Metadata Extraction (REST API + Optional Repository Usage Stats)
=========================================================================
Extracts Tableau Server metadata via REST API and outputs JSON files for
migration analysis. Optionally enriches with view-level usage statistics
from the PostgreSQL readonly repository.

Outputs:
    workbooks.json                - All workbooks across all sites
    views.json                    - All views across all sites
    workbook_datasources.json     - Datasources/connections per workbook
    workbook_views_usage.json     - Views per workbook with usage stats

Environment Variables:
    TABLEAU_PAT_NAME              - Personal Access Token name (required)
    TABLEAU_PAT_SECRET            - Personal Access Token secret (required)
      OR TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET (fallback)
    TABLEAU_SERVER_URL            - Server URL (required)
    TABLEAU_API_VERSION           - REST API version (default: 3.25)
    TABLEAU_REPO_HOST             - Repo hostname (optional, enables usage stats)
    TABLEAU_REPO_PORT             - PostgreSQL port (default: 8060)
    TABLEAU_REPO_USER             - PostgreSQL readonly username (optional)
    TABLEAU_REPO_PASSWORD         - Password (optional)
    TABLEAU_OUTPUT_DIR            - Output directory (default: ./output/metadata)
    TABLEAU_SITES                 - Comma-separated site contentUrls (default: Default)
"""

import os
import sys
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests

# ============================================================================
# Configuration
# ============================================================================

# ============================================================================
# Configuration — all environment-driven, no hardcoded infrastructure values
# ============================================================================

TABLEAU_SERVER_URL = os.environ["TABLEAU_SERVER_URL"]  # Required
TABLEAU_API_VERSION = os.getenv("TABLEAU_API_VERSION", "3.25")

TABLEAU_REPO_HOST = os.getenv("TABLEAU_REPO_HOST", "")
TABLEAU_REPO_PORT = int(os.getenv("TABLEAU_REPO_PORT", "8060"))
TABLEAU_REPO_DB = os.getenv("TABLEAU_REPO_DB", "workgroup")

OUTPUT_DIR = os.getenv("TABLEAU_OUTPUT_DIR", os.path.join(os.getcwd(), "output", "metadata"))
SITES_FILTER = tuple(s.strip() for s in os.getenv("TABLEAU_SITES", "Default").split(","))

# ============================================================================
# Logging
# ============================================================================

def setup_logging(output_dir: str) -> logging.Logger:
    logger = logging.getLogger("tableau_metadata")
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    fh = logging.FileHandler(os.path.join(output_dir, f"metadata_extract_{ts}.log"), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    return logger

# ============================================================================
# REST API
# ============================================================================

class TableauRestAPI:
    """Thin wrapper around Tableau Server REST API."""

    def __init__(self, server_url: str, api_version: str, logger: logging.Logger):
        self.base_url = f"{server_url.rstrip('/')}/api/{api_version}"
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self.auth_token: Optional[str] = None
        self.site_id: Optional[str] = None
        self.site_name: Optional[str] = None

    def sign_in(self, site_content_url: str, pat_name: str, pat_secret: str) -> bool:
        payload = {
            "credentials": {
                "personalAccessTokenName": pat_name,
                "personalAccessTokenSecret": pat_secret,
                "site": {"contentUrl": site_content_url},
            }
        }
        try:
            resp = self.session.post(f"{self.base_url}/auth/signin", json=payload, timeout=30)
            resp.raise_for_status()
            creds = resp.json()["credentials"]
            token: str = creds["token"]
            self.auth_token = token
            self.site_id = creds["site"]["id"]
            self.site_name = creds["site"].get("contentUrl", site_content_url)
            self.session.headers["X-Tableau-Auth"] = token
            return True
        except Exception as e:
            self.logger.error(f"API sign-in failed for site '{site_content_url}': {e}")
            return False

    def sign_out(self):
        if self.auth_token:
            try:
                self.session.post(f"{self.base_url}/auth/signout", timeout=10)
            except Exception:
                pass
        self.auth_token = None
        self.site_id = None
        self.site_name = None
        self.session.headers.pop("X-Tableau-Auth", None)

    def close(self):
        self.session.close()

    def _get_paginated(self, endpoint: str, root_key: str, item_key: str) -> list:
        all_items: list = []
        page_number = 1
        page_size = 100
        expected_total = None

        while True:
            sep = "&" if "?" in endpoint else "?"
            url = f"{self.base_url}/sites/{self.site_id}/{endpoint}{sep}pageSize={page_size}&pageNumber={page_number}"
            try:
                resp = self.session.get(url, timeout=60)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                self.logger.error(f"API request failed: {endpoint} page {page_number}: {e}")
                if expected_total is not None:
                    self.logger.warning(
                        f"Pagination incomplete for {endpoint}: "
                        f"retrieved {len(all_items)} of {expected_total}"
                    )
                break

            items = data.get(root_key, {}).get(item_key, [])
            if isinstance(items, dict):
                items = [items]
            all_items.extend(items)

            total = int(data.get("pagination", {}).get("totalAvailable", 0))
            if expected_total is None:
                expected_total = total
            if len(all_items) >= total or not items:
                break
            page_number += 1

        return all_items

    def get_workbooks(self) -> list:
        return self._get_paginated("workbooks", "workbooks", "workbook")

    def get_views(self) -> list:
        return self._get_paginated("views", "views", "view")

    def get_workbook_connections(self, workbook_id: str) -> list:
        url = f"{self.base_url}/sites/{self.site_id}/workbooks/{workbook_id}/connections"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            conns = resp.json().get("connections", {}).get("connection", [])
            return [conns] if isinstance(conns, dict) else conns
        except Exception as e:
            self.logger.warning(f"Failed to get connections for workbook {workbook_id}: {e}")
            return []

# ============================================================================
# Repository (optional — usage stats only)
# ============================================================================

USAGE_STATS_QUERY = """
SELECT
    s.url_namespace                     AS site_content_url,
    w.name                              AS workbook_name,
    w.id                                AS workbook_repo_id,
    p.name                              AS project_name,
    v.name                              AS view_name,
    v.title                             AS view_title,
    COALESCE(vs.nviews, 0)              AS total_views,
    vs.last_view_time                   AS last_accessed
FROM _views v
INNER JOIN _workbooks w      ON v.workbook_id = w.id
INNER JOIN _sites s          ON w.site_id = s.id
LEFT JOIN _projects p        ON w.project_id = p.id AND w.site_id = p.site_id
LEFT JOIN _views_stats vs    ON vs.views_id = v.id
WHERE s.url_namespace IN :sites
ORDER BY s.url_namespace, w.name, v.name;
"""


def fetch_usage_stats(sites: tuple, logger: logging.Logger) -> dict:
    """Fetch view-level usage stats from repository.

    Returns dict keyed by (site_content_url, workbook_name, view_name)
    with {total_views, last_accessed}.
    """
    repo_user = os.getenv("TABLEAU_REPO_USER")
    repo_password = os.getenv("TABLEAU_REPO_PASSWORD")
    if not repo_user or not repo_password:
        logger.info("Repo credentials not set — usage stats will not be included.")
        return {}

    try:
        from sqlalchemy import create_engine, text, bindparam
        from sqlalchemy.engine.url import URL
    except ImportError:
        logger.warning("sqlalchemy not installed — usage stats will not be included.")
        return {}

    connection_url = URL.create(
        drivername="postgresql+psycopg2",
        username=repo_user,
        password=repo_password,
        host=TABLEAU_REPO_HOST,
        port=TABLEAU_REPO_PORT,
        database=TABLEAU_REPO_DB,
    )

    try:
        engine = create_engine(connection_url, connect_args={"connect_timeout": 30})
        with engine.connect() as conn:
            stmt = text(USAGE_STATS_QUERY).bindparams(bindparam("sites", expanding=True))
            result = conn.execute(stmt, {"sites": list(sites)})
            rows = result.fetchall()
            keys = result.keys()
        engine.dispose()
    except Exception as e:
        logger.error(f"Repo query failed — continuing without usage stats: {e}")
        return {}

    logger.info(f"Repo: {len(rows)} view usage records retrieved.")

    stats: dict[tuple, dict] = {}
    for row in rows:
        r = dict(zip(keys, row))
        key = (r["site_content_url"], r["workbook_name"], r["view_name"])
        last = r["last_accessed"]
        stats[key] = {
            "total_views": r["total_views"],
            "last_accessed": last.isoformat() if last else None,
        }
    return stats

# ============================================================================
# Extraction
# ============================================================================

def flatten_workbook(wb: dict, site_content_url: str) -> dict:
    """Flatten API workbook object to a clean dict."""
    project = wb.get("project", {})
    owner = wb.get("owner", {})
    return {
        "id": wb.get("id", ""),
        "name": wb.get("name", ""),
        "content_url": wb.get("contentUrl", ""),
        "site_content_url": site_content_url,
        "project_id": project.get("id", ""),
        "project_name": project.get("name", ""),
        "owner_id": owner.get("id", ""),
        "owner_name": owner.get("name", ""),
        "created_at": wb.get("createdAt", ""),
        "updated_at": wb.get("updatedAt", ""),
        "webpage_url": wb.get("webpageUrl", ""),
        "size": wb.get("size", ""),
        "has_extracts": wb.get("hasExtracts", False),
    }


def flatten_view(view: dict, site_content_url: str) -> dict:
    """Flatten API view object to a clean dict."""
    wb = view.get("workbook", {})
    owner = view.get("owner", {})
    usage = view.get("usage", {})
    return {
        "id": view.get("id", ""),
        "name": view.get("name", ""),
        "content_url": view.get("contentUrl", ""),
        "site_content_url": site_content_url,
        "workbook_id": wb.get("id", ""),
        "workbook_name": wb.get("name", ""),
        "owner_id": owner.get("id", ""),
        "owner_name": owner.get("name", ""),
        "created_at": view.get("createdAt", ""),
        "updated_at": view.get("updatedAt", ""),
        "total_views_api": int(usage.get("totalViewCount", 0)) if usage else 0,
    }


def flatten_connection(conn: dict) -> dict:
    """Flatten API connection object."""
    ds = conn.get("datasource", {})
    return {
        "connection_type": conn.get("type", ""),
        "server_address": conn.get("serverAddress", ""),
        "server_port": conn.get("serverPort", ""),
        "database_name": conn.get("dbName", ""),
        "username": conn.get("userName", ""),
        "datasource_id": ds.get("id", ""),
        "datasource_name": ds.get("name", ""),
    }


def write_json(data: object, filepath: str, logger: logging.Logger):
    """Write data to JSON file with UTF-8 encoding."""
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    logger.info(f"  Written: {filepath}")

# ============================================================================
# Main
# ============================================================================

def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger = setup_logging(OUTPUT_DIR)

    logger.info("=" * 60)
    logger.info("Tableau Metadata Extraction")
    logger.info("=" * 60)

    # --- Credentials ---
    pat_name = os.getenv("TABLEAU_PAT_NAME")
    pat_secret = os.getenv("TABLEAU_PAT_SECRET") or os.getenv("TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET")
    if not pat_name or not pat_secret:
        logger.error("TABLEAU_PAT_NAME and TABLEAU_PAT_SECRET (or TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET) must be set.")
        sys.exit(1)

    logger.info(f"Server: {TABLEAU_SERVER_URL}")
    logger.info(f"Sites: {', '.join(SITES_FILTER)}")

    # --- Optional: repo usage stats ---
    logger.info("Fetching repository usage stats...")
    usage_stats = fetch_usage_stats(SITES_FILTER, logger)
    repo_available = bool(usage_stats)
    logger.info(f"Usage stats: {'available' if repo_available else 'not available (API-only mode)'}")

    # --- API extraction ---
    api = TableauRestAPI(TABLEAU_SERVER_URL, TABLEAU_API_VERSION, logger)
    all_workbooks: list[dict] = []
    all_views: list[dict] = []
    all_wb_datasources: list[dict] = []
    all_wb_views_usage: list[dict] = []

    try:
        for site_url in SITES_FILTER:
            logger.info(f"--- Site: {site_url} ---")
            if not api.sign_in(site_url, pat_name, pat_secret):
                logger.warning(f"Skipping site '{site_url}' — sign-in failed.")
                continue

            try:
                # 1. Workbooks
                workbooks = api.get_workbooks()
                logger.info(f"  Workbooks: {len(workbooks)}")
                wb_lookup: dict[str, dict] = {}
                for wb in workbooks:
                    flat = flatten_workbook(wb, site_url)
                    all_workbooks.append(flat)
                    wb_lookup[flat["id"]] = flat

                # 2. Views
                views = api.get_views()
                logger.info(f"  Views: {len(views)}")
                for v in views:
                    all_views.append(flatten_view(v, site_url))

                # 3. Datasources/connections per workbook
                logger.info(f"  Fetching connections for {len(workbooks)} workbooks...")
                for i, wb in enumerate(workbooks, 1):
                    wb_id = wb.get("id", "")
                    wb_name = wb.get("name", "")
                    project_name = wb.get("project", {}).get("name", "")
                    connections = api.get_workbook_connections(wb_id)
                    all_wb_datasources.append({
                        "workbook_id": wb_id,
                        "workbook_name": wb_name,
                        "project_name": project_name,
                        "site_content_url": site_url,
                        "connections": [flatten_connection(c) for c in connections],
                    })
                    if i % 50 == 0:
                        logger.info(f"  {i}/{len(workbooks)} workbooks processed...")
                    time.sleep(0.05)

                # 4. Views per workbook with usage stats
                # Group views by workbook_id
                views_by_wb: dict[str, list] = {}
                for v in views:
                    vwb_id = v.get("workbook", {}).get("id", "")
                    if vwb_id:
                        views_by_wb.setdefault(vwb_id, []).append(v)

                for wb_id, wb_views in views_by_wb.items():
                    wb_info = wb_lookup.get(wb_id, {})
                    wb_name = wb_info.get("name", "")
                    project_name = wb_info.get("project_name", "")

                    view_records = []
                    for v in wb_views:
                        view_name = v.get("name", "")
                        usage_api = v.get("usage", {})
                        record: dict = {
                            "view_id": v.get("id", ""),
                            "view_name": view_name,
                            "view_title": v.get("name", ""),
                            "content_url": v.get("contentUrl", ""),
                            "total_views_api": int(usage_api.get("totalViewCount", 0)) if usage_api else 0,
                        }
                        # Enrich with repo stats if available
                        if repo_available:
                            key = (site_url, wb_name, view_name)
                            repo_stats = usage_stats.get(key, {})
                            record["total_views_repo"] = repo_stats.get("total_views")
                            record["last_accessed"] = repo_stats.get("last_accessed")
                        view_records.append(record)

                    all_wb_views_usage.append({
                        "workbook_id": wb_id,
                        "workbook_name": wb_name,
                        "project_name": project_name,
                        "site_content_url": site_url,
                        "view_count": len(view_records),
                        "views": view_records,
                    })

                logger.info(f"  Site '{site_url}' complete.")
            finally:
                api.sign_out()
    finally:
        api.close()

    # --- Write JSON outputs ---
    logger.info("Writing JSON files...")
    ts = datetime.now(timezone.utc).strftime("%Y%m%d")

    write_json(all_workbooks, os.path.join(OUTPUT_DIR, f"workbooks_{ts}.json"), logger)
    write_json(all_views, os.path.join(OUTPUT_DIR, f"views_{ts}.json"), logger)
    write_json(all_wb_datasources, os.path.join(OUTPUT_DIR, f"workbook_datasources_{ts}.json"), logger)
    write_json(all_wb_views_usage, os.path.join(OUTPUT_DIR, f"workbook_views_usage_{ts}.json"), logger)

    # --- Summary ---
    logger.info("=" * 60)
    logger.info("Extraction Summary")
    logger.info(f"  Workbooks:        {len(all_workbooks)}")
    logger.info(f"  Views:            {len(all_views)}")
    logger.info(f"  Workbook DS:      {len(all_wb_datasources)}")
    logger.info(f"  Workbook Views:   {len(all_wb_views_usage)}")
    logger.info(f"  Repo usage stats: {'Yes' if repo_available else 'No'}")
    logger.info(f"  Output:           {OUTPUT_DIR}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
