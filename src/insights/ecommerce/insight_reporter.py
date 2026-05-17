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
    static_drivers: dict | None = None,
    generic_eda: dict | None = None,
) -> dict:
    """Assemble a privacy-safe insight payload from module outputs."""
    payload: dict = {
        "kpis": kpis,
        "eda": eda,
        "trends": trends,
        "anomalies": anomalies,
        "drivers": drivers,
    }
    if static_drivers:
        payload["static_drivers"] = static_drivers
    if generic_eda:
        payload["generic_eda"] = generic_eda
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
            max_tokens=1200,
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
    sections.append(_summarize_static_drivers(payload.get("static_drivers", {})))
    sections.append(_summarize_generic_eda(payload.get("generic_eda", {}), payload.get("kpis", {})))
    sections.append(_limitations_section(payload))
    sections.append(_recommended_questions(payload))

    return "\n\n".join(s for s in sections if s)


def _build_ai_prompt(payload: dict) -> str:
    """Build a concise, aggregate-only prompt for the AI model."""
    kpis = payload.get("kpis", {})
    trends = payload.get("trends", {})
    anomalies = payload.get("anomalies", {})
    drivers = payload.get("drivers", {})
    static_drivers = payload.get("static_drivers", {})
    generic_eda = payload.get("generic_eda", {})
    eda = payload.get("eda", {})

    revenue_label = kpis.get("revenue_label", "Revenue")
    rev_source = kpis.get("revenue_source_column", "")
    provenance_note = (
        f"Note: '{rev_source}' is used as the {revenue_label} metric."
        if rev_source else ""
    )

    prompt_parts = [
        "You are a senior business analyst. Write a concise executive report (max 450 words) based ONLY on the aggregate data below.",
        "Use hedged language: 'suggests', 'appears to', 'may indicate', 'should be investigated'.",
        "Never claim causal certainty. Structure: 1) Key Metrics 2) Trend 3) Anomalies 4) Main Drivers 5) Recommended Follow-ups 6) Limitations.",
        "IMPORTANT: Do NOT use dollar signs ($) anywhere — use 'USD' prefix instead (e.g. 'USD 1,234').",
        "Include column provenance where relevant (e.g. 'based on Transaction Value column').",
        provenance_note,
        "",
        f"AGGREGATE DATA (no individual rows):\n{json.dumps({'kpis': kpis, 'trends': trends, 'anomalies': anomalies, 'drivers': drivers, 'static_drivers': static_drivers, 'generic_eda_summary': generic_eda, 'eda_summary': eda}, default=str, indent=2)}",
    ]
    return "\n".join(p for p in prompt_parts if p is not None)


# ---------------------------------------------------------------------------
# Fallback summary sections
# ---------------------------------------------------------------------------

def _fmt_usd(value: float) -> str:
    """Format a monetary value without dollar signs (Streamlit Markdown-safe)."""
    return f"USD {value:,.2f}"


def _summarize_kpis(kpis: dict) -> str:
    if not kpis:
        return ""
    revenue_label = kpis.get("revenue_label", "Total Revenue")
    rev_source = kpis.get("revenue_source_column", "")
    lines = ["### Key Metrics"]
    if "total_revenue" in kpis:
        label = revenue_label
        source_note = f" *(source: {rev_source})*" if rev_source else ""
        lines.append(f"- {label}: **{_fmt_usd(kpis['total_revenue'])}**{source_note}")
    if "total_orders" in kpis:
        lines.append(f"- Total Orders: **{kpis['total_orders']:,}**")
    if "average_order_value" in kpis and kpis["average_order_value"] is not None:
        lines.append(f"- Average Order Value: **{_fmt_usd(kpis['average_order_value'])}**")
    if "unique_customers" in kpis:
        lines.append(f"- Unique Customers: **{kpis['unique_customers']:,}**")
    if "return_rate" in kpis:
        lines.append(f"- Return Rate: **{kpis['return_rate'] * 100:.1f}%**")
    if "churn_rate" in kpis:
        lines.append(f"- Churn Rate: **{kpis['churn_rate'] * 100:.1f}%**")
    return "\n".join(lines)


def _summarize_trends(trends: dict) -> str:
    rev_trend = trends.get("revenue_trend", {})
    if not rev_trend or "error" in rev_trend:
        return ""
    label = rev_trend.get("trend_label", "unknown")
    notes = rev_trend.get("notes", [])
    lines = [f"### Trend Analysis\nRevenue trend appears **{label}**."]
    lines.extend(notes)
    if rev_trend.get("largest_drop"):
        d = rev_trend["largest_drop"]
        lines.append(
            f"Largest single-period decline: **{d['pct_change']:.1f}%** in {d['period']} "
            "— this may indicate a seasonal effect or business disruption and should be investigated."
        )
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
    line = f"### Period-over-Period Drivers\nRevenue suggests a **{direction}** of {_fmt_usd(abs(change))}"
    if pct is not None:
        line += f" ({abs(pct):.1f}%)"
    line += f" from {prev} to {curr}."
    lines = [line]

    for dim in ("category", "product", "region", "channel"):
        key = f"drivers_by_{dim}"
        dim_drivers = drivers.get(key, [])
        if dim_drivers:
            top = dim_drivers[0]
            if top["absolute_change"] < 0:
                lines.append(
                    f"- **{dim.title()}**: '{top['dimension_value']}' may indicate the largest "
                    f"negative contribution ({_fmt_usd(abs(top['absolute_change']))}, "
                    f"{abs(top['contribution_to_total_change_pct']):.1f}% of total change)."
                )
            break
    return "\n".join(lines)


def _summarize_static_drivers(static_drivers: dict) -> str:
    if not static_drivers or "error" in static_drivers:
        return ""
    dims = static_drivers.get("dimensions_analyzed", [])
    if not dims:
        return ""
    lines = ["### Segment Analysis (Static)"]
    lines.append(
        f"Based on available data, the following dimensions were analyzed: "
        f"{', '.join(d.replace('_', ' ').title() for d in dims)}."
    )
    for dim in dims:
        rows = static_drivers.get(f"static_by_{dim}", [])
        if not rows:
            continue
        top = rows[0]
        rev_info = f" — {_fmt_usd(top['total_revenue'])} total revenue" if "total_revenue" in top else ""
        ret_info = f", return rate {top['return_rate']*100:.1f}%" if "return_rate" in top else ""
        churn_info = f", churn rate {top['churn_rate']*100:.1f}%" if "churn_rate" in top else ""
        lines.append(
            f"- Top {dim.replace('_', ' ').title()}: **{top['value']}**{rev_info}{ret_info}{churn_info}."
        )
    warn = static_drivers.get("concentration_warning")
    if warn:
        lines.append(f"- **Concentration risk**: {warn} This may indicate over-reliance and should be investigated.")
    return "\n".join(lines)


def _summarize_generic_eda(generic_eda: dict, kpis: dict) -> str:
    if not generic_eda:
        return ""
    lines = ["### Data Distribution Overview"]
    overall_return = generic_eda.get("overall_return_rate")
    overall_churn = generic_eda.get("overall_churn_rate")
    if overall_return is not None:
        lines.append(f"- Overall return rate appears to be **{overall_return*100:.1f}%** of transactions.")
    if overall_churn is not None:
        lines.append(f"- Overall churn rate appears to be **{overall_churn*100:.1f}%** of customers.")
    correlations = generic_eda.get("numeric_correlations", [])
    if correlations:
        top_corr = correlations[0]
        lines.append(
            f"- Strong correlation ({top_corr['correlation']:+.2f}) detected between "
            f"'{top_corr['col_a']}' and '{top_corr['col_b']}' — may indicate a structural relationship."
        )
    if len(lines) == 1:
        return ""
    return "\n".join(lines)


def _limitations_section(payload: dict) -> str:
    eda = payload.get("eda", {})
    kpis = payload.get("kpis", {})
    trends = payload.get("trends", {})
    missing_pct = eda.get("missing_pct", 0)
    lines = ["### Limitations"]
    lines.append("- All findings are based on aggregate statistics and should be validated against source systems.")
    lines.append("- Correlations in this report suggest but do not confirm causal relationships.")
    if missing_pct and missing_pct > 5:
        lines.append(f"- Data contains approximately {missing_pct:.1f}% missing values, which may affect accuracy.")

    # Note if trend analysis was unavailable
    rev_trend = trends.get("revenue_trend", {})
    if not rev_trend or "error" in rev_trend:
        lines.append("- Time-series trend analysis was unavailable (no reliable date or revenue column detected).")
    else:
        lines.append("- Anomaly detection uses statistical thresholds and may surface false positives.")

    # Note revenue proxy
    rev_source = kpis.get("revenue_source_column", "")
    details = payload.get("kpis", {})
    if rev_source:
        revenue_label = kpis.get("revenue_label", "Revenue")
        lines.append(f"- '{rev_source}' was used as the {revenue_label} metric (column provenance).")

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
