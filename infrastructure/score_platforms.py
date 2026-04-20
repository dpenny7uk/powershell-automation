"""
Platform Ranking Rules Engine
==============================
Given a workflow's extracted signals and its scheduling state, return a ranked
list of target platforms (1st / 2nd / 3rd) plus the platforms ruled out by
hard constraints.

Signals dict expected keys (all produced by Analyse-AlteryxWorkflows.parse_workflow):
    has_python_or_r, has_spatial, has_predictive, has_http, has_email,
    has_report_output, has_interface, has_dynamic, has_run_command, has_macros,
    db_io_ratio (0..1), transform_ratio (0..1), sql_query_count (int),
    tool_count (int), connection_hint (str)

Public API:
    rank_platforms(signals, is_scheduled) -> dict with keys:
        "ranked":     list of (platform, tag) tuples in preference order
        "ineligible": list of (platform, reason) tuples, ruled out by hard constraints

Ties are broken by platform priority: Databricks > ADF > Python > PBI — which
matches the centre of gravity for non-trivial work in this stack.
"""

PLATFORM_PRIORITY = ["Databricks", "ADF", "Python", "PBI Dataflows"]


# ── Migration effort estimation ─────────────────────────────────────────────
# Heuristic only: bands based on workflow signals, not observed migration data.
# Calibrate after completing a handful of migrations and comparing predicted
# vs actual.

EFFORT_BANDS_MANUAL = [
    ("XS", "<1 day"),
    ("S",  "1-3 days"),
    ("M",  "3-8 days"),
    ("L",  "8-15 days"),
    ("XL", "15-30 days"),
]

EFFORT_BANDS_AI = [
    ("XS", "1-2 hrs"),
    ("S",  "2-5 hrs"),
    ("M",  "5-12 hrs"),
    ("L",  "12-30 hrs"),
]


def _tool_count_base(tool_count, thresholds):
    """Return band index given tool count and an ascending list of thresholds."""
    for i, limit in enumerate(thresholds):
        if tool_count < limit:
            return i
    return len(thresholds)


def _format_reasons(reasons, limit=4):
    """Trim reasons list for short-tag display."""
    if len(reasons) <= limit:
        return ", ".join(reasons)
    return ", ".join(reasons[:limit]) + f" (+{len(reasons) - limit} more)"


def estimate_manual_effort(signals):
    """
    Estimate person-time for a human engineer to migrate the workflow.
    Returns a short tag string like 'M (3-8 days) - 47 tools, macros, spatial'.
    """
    tool_count = signals.get("tool_count", 0)
    base = _tool_count_base(tool_count, [10, 25, 60, 120])

    reasons = [f"{tool_count} tools"]
    bump = 0.0

    if signals.get("has_macros"):
        bump += 1
        reasons.append("macros")
    if signals.get("has_spatial"):
        bump += 1
        reasons.append("spatial")
    if signals.get("has_predictive"):
        bump += 1
        reasons.append("predictive models")
    elif signals.get("has_python_or_r"):
        # Only count once — predictive usually implies Python/R
        bump += 0.5
        reasons.append("Python/R code")
    if signals.get("has_interface"):
        bump += 1
        reasons.append("analytic app UI")
    if signals.get("has_dynamic"):
        bump += 0.5
        reasons.append("dynamic I/O")
    if signals.get("has_report_output"):
        bump += 0.5
        reasons.append("report rendering")
    sql_n = signals.get("sql_query_count", 0)
    if sql_n >= 5:
        bump += 0.5
        reasons.append(f"{sql_n} SQL queries")
    if signals.get("connection_hint") == "mixed":
        bump += 0.5
        reasons.append("mixed connections")

    final_band = min(int(base + bump), len(EFFORT_BANDS_MANUAL) - 1)
    label, duration = EFFORT_BANDS_MANUAL[final_band]
    return f"{label} ({duration}) \u2014 {_format_reasons(reasons)}"


def estimate_ai_effort(signals):
    """
    Estimate AI-agent time + human review for the same workflow.
    Returns a short tag, or 'Not suitable - <reason>' when the workflow is
    beyond what an AI agent can reliably migrate.
    """
    tool_count = signals.get("tool_count", 0)

    # Hard disqualifiers
    if signals.get("has_interface"):
        return "Not suitable \u2014 analytic app UI (product decision, not translation)"
    if tool_count > 150:
        return f"Not suitable \u2014 {tool_count} tools (context limits, cascading errors)"
    if signals.get("has_macros") and signals.get("has_predictive"):
        return "Not suitable \u2014 macros + predictive models (compound complexity)"

    base = _tool_count_base(tool_count, [10, 30, 75])  # caps at band index 3 (L)

    reasons = [f"{tool_count} tools"]
    bump = 0.0

    if signals.get("has_macros"):
        bump += 1
        reasons.append("macros (cross-file context)")
    if signals.get("has_spatial"):
        bump += 1
        reasons.append("spatial (custom libs)")
    if signals.get("has_predictive"):
        bump += 1
        reasons.append("predictive (tuning needed)")
    elif signals.get("has_python_or_r"):
        bump += 0.5
        reasons.append("Python/R code")
    if signals.get("has_dynamic"):
        bump += 0.5
        reasons.append("dynamic I/O")
    if signals.get("has_report_output"):
        bump += 0.5
        reasons.append("report rendering")
    sql_n = signals.get("sql_query_count", 0)
    if sql_n >= 10:
        bump += 0.5
        reasons.append(f"{sql_n} SQL queries")

    final_band = min(int(base + bump), len(EFFORT_BANDS_AI) - 1)
    label, duration = EFFORT_BANDS_AI[final_band]
    return f"{label} ({duration} + human review) \u2014 {_format_reasons(reasons)}"


def effort_band(effort_tag):
    """Extract the band letter (XS/S/M/L/XL) from an effort tag, or 'N/A'."""
    if not effort_tag or effort_tag.startswith("Not suitable"):
        return "Not suitable" if effort_tag and effort_tag.startswith("Not suitable") else "N/A"
    # Tag format: 'XS (...) - reasons'
    return effort_tag.split(" ", 1)[0]


def _score_databricks(s):
    """Score Databricks. Returns (score, tag_parts, disqualifier)."""
    if s.get("has_interface"):
        return 0, [], "no UI replacement for analytic app"

    score = 0
    parts = []

    if s.get("has_python_or_r"):
        score += 3
        parts.append("Python/R Tool present")
    if s.get("has_spatial"):
        score += 3
        parts.append("spatial tools")
    if s.get("has_predictive"):
        score += 3
        parts.append("predictive models")
    if s.get("tool_count", 0) > 50:
        score += 2
        parts.append("large workflow")
    if s.get("has_macros"):
        score += 2
        parts.append("macro dependencies")
    if s.get("sql_query_count", 0) >= 3:
        score += 2
        parts.append(f"{s['sql_query_count']} SQL queries")
    if s.get("has_dynamic"):
        score += 1
        parts.append("dynamic I/O")
    if s.get("transform_ratio", 0) > 0.4:
        score += 1
        parts.append("heavy transforms")

    return score, parts, None


def _score_adf(s):
    """Score Azure Data Factory. Returns (score, tag_parts, disqualifier)."""
    if s.get("has_python_or_r"):
        return 0, [], "has Python/R Tool (ADF can't run inline code)"
    if s.get("has_spatial"):
        return 0, [], "has spatial tools"
    if s.get("has_predictive"):
        return 0, [], "has predictive tools"
    if s.get("has_report_output"):
        return 0, [], "has report rendering (no PDF/Excel output in ADF)"
    if s.get("has_interface"):
        return 0, [], "no UI replacement for analytic app"

    score = 0
    parts = []

    if s.get("db_io_ratio", 0) > 0.3:
        score += 3
        parts.append("DB-heavy I/O")
    if s.get("is_scheduled"):
        score += 2
        parts.append("scheduled")
    hint = s.get("connection_hint", "")
    if hint in ("sqlserver", "oracle"):
        score += 2
        parts.append(f"{hint} source")
    if s.get("has_http") and not s.get("has_report_output"):
        score += 1
        parts.append("HTTP ingest")
    if s.get("transform_ratio", 1) < 0.3:
        score += 1
        parts.append("low transform complexity")

    return score, parts, None


def _score_python(s):
    """Score standalone Python. Returns (score, tag_parts, disqualifier)."""
    if s.get("has_interface"):
        return 0, [], "no UI replacement for analytic app"
    if s.get("tool_count", 0) > 100:
        return 0, [], "too large for maintainable script"
    if s.get("has_predictive"):
        return 0, [], "predictive models belong in Databricks"

    score = 0
    parts = []

    if s.get("has_report_output"):
        score += 3
        parts.append("report rendering (openpyxl/reportlab fit)")
    if s.get("has_http") and s.get("tool_count", 0) <= 30:
        score += 3
        parts.append("HTTP ingest + small footprint")
    if s.get("tool_count", 0) <= 30:
        score += 2
        parts.append("small workflow")
    if s.get("has_email"):
        score += 2
        parts.append("email output")
    if s.get("connection_hint") == "flatfile":
        score += 2
        parts.append("flat-file I/O")
    if not s.get("is_scheduled"):
        score += 1
        parts.append("ad-hoc pattern")

    return score, parts, None


def _score_pbi(s):
    """Score Power BI Dataflows / Fabric. Returns (score, tag_parts, disqualifier)."""
    # Long list of hard disqualifiers — PBI Dataflows is narrowly applicable.
    if s.get("has_python_or_r"):
        return 0, [], "has Python/R Tool"
    if s.get("has_spatial"):
        return 0, [], "has spatial tools"
    if s.get("has_predictive"):
        return 0, [], "has predictive tools"
    if s.get("has_report_output"):
        return 0, [], "has report rendering"
    if s.get("has_interface"):
        return 0, [], "no UI replacement for analytic app"
    if s.get("has_macros"):
        return 0, [], "has macro dependencies"
    if s.get("has_dynamic"):
        return 0, [], "has dynamic I/O"
    if s.get("has_run_command"):
        return 0, [], "has Run Command (shell-out)"
    if s.get("tool_count", 0) > 30:
        return 0, [], "too large for PBI Dataflow"

    score = 0
    parts = []

    if s.get("tool_count", 0) <= 15 and s.get("transform_ratio", 1) <= 0.5:
        score += 2
        parts.append("small + simple transforms")
    if s.get("connection_hint") == "sqlserver":
        score += 1
        parts.append("SQL Server source")
    if s.get("db_io_ratio", 0) > 0.2:
        score += 1
        parts.append("BI-facing DB pull")

    return score, parts, None


def _format_tag(platform, parts):
    """Produce a short tag like 'ADF — SQL Server source, scheduled'."""
    if parts:
        return f"{platform} \u2014 {', '.join(parts)}"
    return f"{platform} \u2014 default fit"


def rank_platforms(signals, is_scheduled):
    """
    Rank the four target platforms for a single workflow.

    Returns:
        {
            "ranked":     [(platform, tag), ...]  # in preference order
            "ineligible": [(platform, reason), ...]
        }
    """
    # Inject is_scheduled into the signals dict so scorers see a single source
    s = dict(signals)
    s["is_scheduled"] = bool(is_scheduled)

    scorers = {
        "Databricks":    _score_databricks,
        "ADF":           _score_adf,
        "Python":        _score_python,
        "PBI Dataflows": _score_pbi,
    }

    results = {}      # platform -> (score, tag)
    ineligible = []

    for platform, scorer in scorers.items():
        score, parts, disqualifier = scorer(s)
        if disqualifier is not None:
            ineligible.append((platform, disqualifier))
        elif score <= 0:
            ineligible.append((platform, "no matching rules fired"))
        else:
            results[platform] = (score, _format_tag(platform, parts))

    # Sort eligible platforms: score desc, then by priority order
    priority_index = {p: i for i, p in enumerate(PLATFORM_PRIORITY)}
    ordered = sorted(
        results.items(),
        key=lambda kv: (-kv[1][0], priority_index.get(kv[0], 99)),
    )

    ranked = [(platform, tag) for platform, (_score, tag) in ordered]

    return {"ranked": ranked, "ineligible": ineligible}
