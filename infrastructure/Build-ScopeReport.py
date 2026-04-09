"""
Alteryx Migration - Final Scoped Report
========================================
Cross-references schedules, metadata, and the migration analysis
to produce a final report scoped to only active workflows.

Usage:
    python Build-ScopedReport.py C:\Dev\AlteryxExport

Expects these files in the directory:
    - schedules.csv          (from Export-AlteryxSchedules.ps1)
    - workflow_metadata.csv  (from Get-AlteryxMetadata.ps1)
    - migration_report.xlsx  (from Analyse-AlteryxWorkflows.py)

Output:
    - scoped_migration_report.xlsx
"""

import sys, os
import pandas as pd
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

def main():
    if len(sys.argv) < 2:
        print("Usage: python Build-ScopedReport.py <directory>")
        sys.exit(1)

    base = sys.argv[1]
    sched_path = os.path.join(base, "schedules.csv")
    meta_path = os.path.join(base, "workflow_metadata.csv")
    report_path = os.path.join(base, "migration_report.xlsx")
    output_path = os.path.join(base, "scoped_migration_report.xlsx")

    for f, label in [(sched_path, "schedules.csv"), (meta_path, "workflow_metadata.csv"), (report_path, "migration_report.xlsx")]:
        if not os.path.exists(f):
            print(f"ERROR: {label} not found in {base}")
            sys.exit(1)

    # ── Load data ─────────────────────────────────────────────────────────
    print("Loading schedules...")
    schedules = pd.read_csv(sched_path)

    print("Loading metadata...")
    metadata = pd.read_csv(meta_path)

    print("Loading migration report...")
    inventory = pd.read_excel(report_path, sheet_name="Workflow Inventory")
    tool_usage = pd.read_excel(report_path, sheet_name="Tool Usage")

    # ── Get unique scheduled workflow IDs ─────────────────────────────────
    all_scheduled_ids = set(schedules["workflowId"].dropna().unique())
    print(f"Unique scheduled workflows (all): {len(all_scheduled_ids)}")

    # Filter to enabled schedules only if the column exists
    if "enabled" in schedules.columns:
        enabled_schedules = schedules[schedules["enabled"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])]
        disabled_schedules = schedules[~schedules["enabled"].astype(str).str.strip().str.upper().isin(["TRUE", "1"])]
        scheduled_ids = set(enabled_schedules["workflowId"].dropna().unique())
        disabled_ids = set(disabled_schedules["workflowId"].dropna().unique()) - scheduled_ids
        print(f"Unique scheduled workflows (enabled): {len(scheduled_ids)}")
        print(f"Unique scheduled workflows (disabled only): {len(disabled_ids)}")
    else:
        scheduled_ids = all_scheduled_ids
        disabled_ids = set()
        print("  (no 'enabled' column found — treating all as enabled)")

    print(f"Using {len(scheduled_ids)} enabled scheduled workflows as primary scope")

    # ── Classify all workflows ────────────────────────────────────────────
    metadata["is_scheduled"] = metadata["id"].isin(scheduled_ids)

    # Convert runCount to numeric, coerce errors to 0
    metadata["runCount"] = pd.to_numeric(metadata["runCount"], errors="coerce").fillna(0).astype(int)

    # Parse lastJobDate
    metadata["lastJobDate_parsed"] = pd.to_datetime(metadata["lastJobDate"], errors="coerce", dayfirst=True)

    # Active but unscheduled: has run count > 0 OR has a job date, but no schedule
    metadata["is_active_unscheduled"] = (
        ~metadata["is_scheduled"] &
        (
            (metadata["runCount"] > 0) |
            (metadata["lastJobDate_parsed"].notna())
        )
    )

    # Classification
    def classify_activity(row):
        if row["is_scheduled"]:
            return "Scheduled"
        elif row["is_active_unscheduled"]:
            # Sub-classify by recency
            if pd.notna(row["lastJobDate_parsed"]) and row["lastJobDate_parsed"] >= pd.Timestamp("2024-01-01"):
                return "Active (Unscheduled - Recent)"
            elif row["runCount"] > 0:
                return "Active (Unscheduled - Historic)"
            else:
                return "Active (Unscheduled - Has Jobs)"
        else:
            return "Inactive"

    metadata["activity_status"] = metadata.apply(classify_activity, axis=1)

    # Print breakdown
    print("\n── Activity Classification ──")
    for status, count in metadata["activity_status"].value_counts().items():
        print(f"  {status}: {count}")

    # Scope: everything that isn't Inactive
    active_meta = metadata[metadata["activity_status"] != "Inactive"].copy()
    print(f"\nTotal in scope (all active): {len(active_meta)}")

    # ── Schedule summary per workflow ─────────────────────────────────────
    sched_summary = schedules.groupby("workflowId").agg(
        schedule_count=("id", "count"),
        schedule_names=("name", lambda x: " | ".join(x.unique())),
        next_run=("runDateTime", "max")
    ).reset_index()

    active_meta = active_meta.merge(sched_summary, left_on="id", right_on="workflowId", how="left", suffixes=("", "_sched"))

    # ── Match to migration analysis by workflow name ──────────────────────
    # The migration report uses workflow names, metadata uses names too
    # Try to match on name
    inv_lookup = {}
    for _, row in inventory.iterrows():
        inv_lookup[str(row.get("Workflow Name", "")).strip().lower()] = row

    matched = []
    unmatched_names = []

    for _, row in active_meta.iterrows():
        wf_name = str(row.get("name", "")).strip()
        wf_name_lower = wf_name.lower()

        inv_row = inv_lookup.get(wf_name_lower)

        # Try partial match if exact fails
        if inv_row is None:
            for inv_name, inv_data in inv_lookup.items():
                if wf_name_lower in inv_name or inv_name in wf_name_lower:
                    inv_row = inv_data
                    break

        matched.append({
            "Workflow Name": wf_name,
            "Workflow ID": row.get("id", ""),
            "Activity Status": row.get("activity_status", ""),
            "Date Created": row.get("dateCreated", ""),
            "Run Count": row.get("runCount", 0),
            "Package Type": row.get("packageType", ""),
            "Published": row.get("published", ""),
            "Run Disabled": row.get("runDisabled", ""),
            "Last Job Status": row.get("lastJobStatus", ""),
            "Last Job Date": row.get("lastJobDate", ""),
            "Schedule Count": row.get("schedule_count", 0) if pd.notna(row.get("schedule_count")) else 0,
            "Schedule Names": row.get("schedule_names", "") if pd.notna(row.get("schedule_names")) else "",
            "Next Scheduled Run": row.get("next_run", "") if pd.notna(row.get("next_run")) else "",
            "Tier": inv_row["Tier"] if inv_row is not None else "Not in analysis",
            "Tool Count": inv_row["Tool Count"] if inv_row is not None else "",
            "Tools Used": inv_row["Tools Used"] if inv_row is not None else "",
            "Data Connections": inv_row["Data Connections"] if inv_row is not None else "",
            "Macros": inv_row["Macros"] if inv_row is not None else "",
            "SQL Query Count": inv_row["SQL Query Count"] if inv_row is not None else "",
        })

        if inv_row is None:
            unmatched_names.append(wf_name)

    df = pd.DataFrame(matched)
    print(f"\nTotal active workflows: {len(df)}")
    if unmatched_names:
        print(f"Workflows not matched to analysis: {len(unmatched_names)}")
        for n in unmatched_names[:10]:
            print(f"  - {n}")
        if len(unmatched_names) > 10:
            print(f"  ... and {len(unmatched_names) - 10} more")

    # ── Tier breakdown ────────────────────────────────────────────────────
    print("\n── Active Workflow Tier Breakdown ──")
    tier_counts = df["Tier"].value_counts()
    for tier, count in tier_counts.items():
        print(f"  {tier}: {count}")

    print("\n── By Activity Status ──")
    activity_counts = df["Activity Status"].value_counts()
    for status, count in activity_counts.items():
        print(f"  {status}: {count}")

    # ── Build Excel report ────────────────────────────────────────────────
    wb = Workbook()

    header_font = Font(bold=True, color="FFFFFF", size=11)
    header_fill = PatternFill("solid", fgColor="2F5496")
    tier1_fill = PatternFill("solid", fgColor="C6EFCE")
    tier2_fill = PatternFill("solid", fgColor="FFEB9C")
    tier3_fill = PatternFill("solid", fgColor="FFC7CE")
    tier4_fill = PatternFill("solid", fgColor="D9E1F2")
    nomatch_fill = PatternFill("solid", fgColor="F2F2F2")
    scheduled_fill = PatternFill("solid", fgColor="C6EFCE")
    recent_fill = PatternFill("solid", fgColor="FFEB9C")
    historic_fill = PatternFill("solid", fgColor="FFC7CE")
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )
    activity_fills = {
        "Scheduled": scheduled_fill,
        "Active (Unscheduled - Recent)": recent_fill,
        "Active (Unscheduled - Historic)": historic_fill,
        "Active (Unscheduled - Has Jobs)": recent_fill,
    }
    tier_fills = {
        "Tier 1 - Simple ETL": tier1_fill,
        "Tier 2 - Transform & Orchestrate": tier2_fill,
        "Tier 3 - Predictive/Spatial/Code": tier3_fill,
        "Tier 4 - Self-Service/Interface": tier4_fill,
        "Not in analysis": nomatch_fill,
    }

    # ── Sheet 1: Executive Summary ────────────────────────────────────────
    ws = wb.active
    ws.title = "Summary"

    summary_rows = [
        ["Alteryx Migration - Scoped Active Workflows", ""],
        ["", ""],
        ["Total Workflows in Gallery", len(metadata)],
        ["", ""],
        ["Activity Classification", "Count"],
        ["Scheduled (must migrate)", int(metadata["activity_status"].eq("Scheduled").sum())],
        ["  of which: enabled schedules", int(len(scheduled_ids))],
        ["  of which: disabled schedule only", int(len(disabled_ids))],
        ["Active - Unscheduled, Recent (ran since 2024)", int(metadata["activity_status"].eq("Active (Unscheduled - Recent)").sum())],
        ["Active - Unscheduled, Historic (has runs, no recent jobs)", int(metadata["activity_status"].eq("Active (Unscheduled - Historic)").sum())],
        ["Active - Unscheduled, Has Jobs", int(metadata["activity_status"].eq("Active (Unscheduled - Has Jobs)").sum())],
        ["Inactive (no runs, no jobs, no schedule)", int(metadata["activity_status"].eq("Inactive").sum())],
        ["", ""],
        ["Total In Scope (all active)", len(df)],
        ["Workflows Matched to Analysis", len(df) - len(unmatched_names)],
        ["Workflows Not Matched (missing .yxzp)", len(unmatched_names)],
        ["", ""],
        ["Active Workflow Tier Breakdown", "Count"],
    ]
    for tier in ["Tier 1 - Simple ETL", "Tier 2 - Transform & Orchestrate",
                 "Tier 3 - Predictive/Spatial/Code", "Tier 4 - Self-Service/Interface",
                 "Not in analysis"]:
        summary_rows.append([tier, tier_counts.get(tier, 0)])

    summary_rows += [
        ["", ""],
        ["Platform Recommendation", ""],
        ["Tier 1 - Simple ETL", "Any platform: ADF, Python, Power BI Dataflows, Databricks"],
        ["Tier 2 - Transform & Orchestrate", "ADF or Databricks (pipeline orchestration needed)"],
        ["Tier 3 - Predictive/Spatial/Code", "Databricks or standalone Python"],
        ["Tier 4 - Self-Service/Interface", "Needs product decision - user-facing capability"],
        ["Not in analysis", "Review manually - .yxzp not in export or name mismatch"],
    ]

    for row_idx, row_data in enumerate(summary_rows, 1):
        for col_idx, val in enumerate(row_data, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.border = thin_border
            if row_idx == 1:
                cell.font = Font(bold=True, size=14)
            elif row_idx == 8 or row_idx == len(summary_rows) - 6:
                cell.font = header_font
                cell.fill = header_fill

    ws.column_dimensions["A"].width = 50
    ws.column_dimensions["B"].width = 65

    # ── Sheet 2: Active Workflow Detail ───────────────────────────────────
    ws2 = wb.create_sheet("Active Workflows")
    headers = list(df.columns)

    for col_idx, h in enumerate(headers, 1):
        cell = ws2.cell(row=1, column=col_idx, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = thin_border

    for row_idx, (_, row) in enumerate(df.iterrows(), 2):
        for col_idx, h in enumerate(headers, 1):
            val = row[h]
            if pd.isna(val):
                val = ""
            cell = ws2.cell(row=row_idx, column=col_idx, value=val)
            cell.border = thin_border
            cell.alignment = Alignment(wrap_text=True, vertical="top")
            if h == "Tier" and val in tier_fills:
                cell.fill = tier_fills[val]
            if h == "Activity Status" and val in activity_fills:
                cell.fill = activity_fills[val]

    ws2.auto_filter.ref = f"A1:{get_column_letter(len(headers))}1"
    col_widths = [35, 28, 30, 18, 12, 14, 10, 12, 14, 18, 14, 40, 20, 32, 12, 50, 35, 30, 14]
    for col_idx, w in enumerate(col_widths[:len(headers)], 1):
        ws2.column_dimensions[get_column_letter(col_idx)].width = w

    # ── Sheet 3: Unmatched Workflows ──────────────────────────────────────
    if unmatched_names:
        ws3 = wb.create_sheet("Unmatched")
        ws3.cell(row=1, column=1, value="Workflow Name").font = header_font
        ws3.cell(row=1, column=1).fill = header_fill
        ws3.cell(row=1, column=2, value="Notes").font = header_font
        ws3.cell(row=1, column=2).fill = header_fill
        for row_idx, name in enumerate(unmatched_names, 2):
            ws3.cell(row=row_idx, column=1, value=name).border = thin_border
            ws3.cell(row=row_idx, column=2, value="Not found in .yxzp export - review manually").border = thin_border
        ws3.column_dimensions["A"].width = 50
        ws3.column_dimensions["B"].width = 50

    # ── Sheet 4: Schedule Detail ──────────────────────────────────────────
    ws4 = wb.create_sheet("Schedules")
    sched_headers = list(schedules.columns)
    for col_idx, h in enumerate(sched_headers, 1):
        cell = ws4.cell(row=1, column=col_idx, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = thin_border

    for row_idx, (_, row) in enumerate(schedules.iterrows(), 2):
        for col_idx, h in enumerate(sched_headers, 1):
            val = row[h]
            if pd.isna(val):
                val = ""
            cell = ws4.cell(row=row_idx, column=col_idx, value=val)
            cell.border = thin_border

    for col_idx in range(1, len(sched_headers) + 1):
        ws4.column_dimensions[get_column_letter(col_idx)].width = 30

    wb.save(output_path)
    print(f"\nScoped report saved: {output_path}")


if __name__ == "__main__":
    main()