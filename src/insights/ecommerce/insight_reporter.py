"""Executive insight report builder with privacy guards and optional AI narrative."""
from __future__ import annotations

import os
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

_LANGUAGE_HEDGES = [
    "suggests", "appears to", "may indicate", "should be investigated",
    "likely", "seems", "based on available data",
]


def build_insight_payload(
    kpis: dict,
    eda: dict,
    trends: dict,
    anomalies: dict | list,
    drivers: dict,
) -> dict:
    """Assemble a privacy-safe insight payload from module outputs."""
    payload = {
        "kpis": kpis,
        "eda": eda,
        "trends": trends,
        "anomalies": anomalies,
        "drivers": drivers,
    }
    validate_no_raw_rows_in_insight_payload(payload)
    return payload


def validate_no_raw_rows_in_insight_payload(payload: dict) -> None:
    """Raise ValueError if the payload appears to contain individual row data."""
    _check_for_blocked_keys(payload)
    _validate_no_list_of_dicts_with_row_data(payload)


def _check_for_blocked_keys(obj: Any, depth: int = 0) -> None:
    """Recursively raise if any blocked raw-row key is found."""
    _BLOCKED = {"row_indices", "raw_rows", "sample_rows", "individual_records"}
    if depth > 12:
        return
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in _BLOCKED:
                raise ValueError(
                    f"Insight payload contains blocked key '{k}' — "
                    "raw row data must never be sent to an LLM."
                )
            _check_for_blocked_keys(v, depth + 1)
    elif isinstance(obj, list):
        for item in obj:
            _check_for_blocked_keys(item, depth + 1)



def _validate_no_list_of_dicts_with_row_data(obj: Any, depth: int = 0) -> None:
    """Check that no list contains dicts with row-level granularity (too many unique items)."""
    if depth > 8:
        return
    if isinstance(obj, list):
        if len(obj) > 500 and all(isinstance(x, dict) for x in obj[:10]):
            raise ValueError(
                "Payload contains a large list of dicts — likely raw row data. "
                "Only aggregate summaries may be sent to the AI."
            )
        for item in obj:
            _validate_no_list_of_dicts_with_row_data(item, depth + 1)
    elif isinstance(obj, dict):
        for v in obj.values():
            _validate_no_list_of_dicts_with_row_data(v, depth + 1)


def generate_executive_summary(payload: dict, use_ai: bool = True) -> str:
    """Generate executive summary using Claude API, falling back to template if unavailable."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    if not api_key or not use_ai:
        return generate_fallback_summary(payload)

    validate_no_raw_rows_in_insight_payload(payload)

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        prompt = _build_ai_prompt(payload)
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        return message.content[0].text
    except Exception as exc:
        logger.warning("[insight_reporter] AI summary failed (%s), using fallback.", exc)
        return generate_fallback_summary(payload)


def generate_fallback_summary(payload: dict) -> str:
    """Generate a deterministic executive summary from aggregated payload — no API calls."""
    sections: list[str] = []

    sections.append("## Executive Summary\n")
    sections.append(_summarize_kpis(payload.get("kpis", {})))
    sections.append(_summarize_trends(payload.get("trends", {})))
    sections.append(_summarize_anomalies(payload.get("anomalies", {})))
    sections.append(_summarize_drivers(payload.get("drivers", {})))
    sections.append(_limitations_section(payload))
    sections.append(_recommended_questions(payload))

    return "\n\n".join(s for s in sections if s)


def _build_ai_prompt(payload: dict) -> str:
    """Build a concise, aggregate-only prompt for the AI model."""
    kpis = payload.get("kpis", {})
    trends = payload.get("trends", {})
    anomalies = payload.get("anomalies", {})
    drivers = payload.get("drivers", {})
    eda = payload.get("eda", {})

    prompt_parts = [
        "You are a senior business analyst. Write a concise executive report (max 400 words) based ONLY on the aggregate data below.",
        "Use hedged language: 'suggests', 'appears to', 'may indicate', 'should be investigated'.",
        "Never claim causal certainty. Structure: 1) Key Metrics 2) Trend 3) Anomalies 4) Main Drivers 5) Recommended Follow-ups 6) Limitations.",
        "",
        f"AGGREGATE DATA (no individual rows):\n{json.dumps({'kpis': kpis, 'trends': trends, 'anomalies': anomalies, 'drivers': drivers, 'eda_summary': eda}, default=str, indent=2)}",
    ]
    return "\n".join(prompt_parts)


def _summarize_kpis(kpis: dict) -> str:
    if not kpis:
        return ""
    lines = ["### Key Metrics"]
    if "total_revenue" in kpis:
        lines.append(f"- Total Revenue: **${kpis['total_revenue']:,.2f}**")
    if "total_orders" in kpis:
        lines.append(f"- Total Orders: **{kpis['total_orders']:,}**")
    if "average_order_value" in kpis and kpis["average_order_value"] is not None:
        lines.append(f"- Average Order Value: **${kpis['average_order_value']:,.2f}**")
    if "unique_customers" in kpis:
        lines.append(f"- Unique Customers: **{kpis['unique_customers']:,}**")
    if "return_rate" in kpis:
        lines.append(f"- Return Rate: **{kpis['return_rate'] * 100:.1f}%**")
    return "\n".join(lines)


def _summarize_trends(trends: dict) -> str:
    rev_trend = trends.get("revenue_trend", {})
    if not rev_trend or "error" in rev_trend:
        return ""
    label = rev_trend.get("trend_label", "unknown")
    latest = rev_trend.get("latest_period", "")
    notes = rev_trend.get("notes", [])
    lines = [f"### Trend Analysis\nRevenue trend appears **{label}**."]
    lines.extend(notes)
    if rev_trend.get("largest_drop"):
        d = rev_trend["largest_drop"]
        lines.append(f"Largest single-period decline: **{d['pct_change']:.1f}%** in {d['period']} — this may indicate a seasonal effect or business disruption and should be investigated.")
    return "\n".join(lines)


def _summarize_anomalies(anomalies: dict | list) -> str:
    if not anomalies:
        return ""
    if isinstance(anomalies, list):
        anomaly_list = anomalies
    else:
        anomaly_list = [a for v in anomalies.values() if isinstance(v, list) for a in v]
    if not anomaly_list:
        return ""
    high = [a for a in anomaly_list if a.get("severity") == "high"]
    lines = [f"### Anomalies Detected\n{len(anomaly_list)} anomalies detected ({len(high)} high-severity)."]
    for a in anomaly_list[:5]:
        lines.append(f"- [{a.get('severity', '').upper()}] {a.get('explanation_ready_summary', '')}")
    return "\n".join(lines)


def _summarize_drivers(drivers: dict) -> str:
    if not drivers or "error" in drivers:
        return ""
    overall = drivers.get("overall_change", {})
    if not overall or "error" in overall:
        return ""
    change = overall.get("change", 0)
    pct = overall.get("change_pct")
    prev = overall.get("previous_period", "")
    curr = overall.get("latest_period", "")
    direction = "increase" if change >= 0 else "decline"
    lines = [f"### Main Business Drivers\nRevenue suggests a **{direction}** of ${abs(change):,.2f}"]
    if pct is not None:
        lines[0] += f" ({abs(pct):.1f}%)"
    lines[0] += f" from {prev} to {curr}."

    for dim in ("category", "product", "region", "channel"):
        key = f"drivers_by_{dim}"
        dim_drivers = drivers.get(key, [])
        if dim_drivers:
            top = dim_drivers[0]
            if top["absolute_change"] < 0:
                lines.append(
                    f"- **{dim.title()}**: '{top['dimension_value']}' may indicate the largest negative contribution "
                    f"(${abs(top['absolute_change']):,.2f}, {abs(top['contribution_to_total_change_pct']):.1f}% of total change)."
                )
            break
    return "\n".join(lines)


def _limitations_section(payload: dict) -> str:
    eda = payload.get("eda", {})
    missing_pct = eda.get("missing_pct", 0)
    lines = ["### Limitations"]
    lines.append("- All findings are based on aggregate statistics and should be validated against source systems.")
    lines.append("- Correlations in this report suggest but do not confirm causal relationships.")
    if missing_pct and missing_pct > 5:
        lines.append(f"- Data contains approximately {missing_pct:.1f}% missing values, which may affect accuracy.")
    lines.append("- Anomaly detection uses statistical thresholds and may surface false positives.")
    return "\n".join(lines)


def _recommended_questions(payload: dict) -> str:
    questions = [
        "### Recommended Follow-up Questions",
        "1. What operational or marketing events explain the largest revenue changes?",
        "2. Are the detected anomalies consistent with known promotions, seasonality, or data entry errors?",
        "3. Which customer segments are driving the category-level revenue shifts?",
        "4. Is the return rate trend correlated with specific products or channels?",
        "5. What data collection gaps should be addressed before the next reporting cycle?",
    ]
    return "\n".join(questions)
