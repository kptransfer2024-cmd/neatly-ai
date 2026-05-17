"""Executive Report PDF generation using fpdf2.

Generates a clean B2B-ready PDF from insight pipeline output.
No raw rows, no dollar signs ($) — uses USD prefix throughout.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

import pandas as pd


def generate_executive_pdf(report: dict, df: pd.DataFrame | None = None) -> bytes:
    """Return raw PDF bytes for the executive report.

    Raises ImportError if fpdf2 is not installed.
    """
    try:
        from fpdf import FPDF
    except ImportError as exc:
        raise ImportError("fpdf2 is required. Run: pip install fpdf2") from exc

    # -----------------------------------------------------------------------
    # Extract data
    # -----------------------------------------------------------------------
    insights       = report.get("insights", {})
    kpis           = insights.get("kpis", {})
    trends         = insights.get("trends", {})
    anomalies      = insights.get("anomalies", {})
    drivers        = insights.get("drivers", {})
    static_drivers = insights.get("static_drivers", {})
    eda            = insights.get("eda", {})
    summary        = report.get("executive_summary", "")

    # -----------------------------------------------------------------------
    # Text helpers
    # -----------------------------------------------------------------------
    # Latin-1 safe replacements for common typographic characters
    _UNICODE_MAP = str.maketrans({
        "—": "--",    # em dash
        "–": "-",     # en dash
        "‘": "'",     # left single quotation mark
        "’": "'",     # right single quotation mark
        "“": '"',     # left double quotation mark
        "”": '"',     # right double quotation mark
        "•": "-",     # bullet
        "…": "...",   # ellipsis
        " ": " ",     # non-breaking space
        "·": "-",     # middle dot
        "→": "->",    # right arrow
        "←": "<-",    # left arrow
        "✓": "OK",    # checkmark
        "×": "x",     # multiplication sign
    })

    def clean(text: str) -> str:
        """Strip markdown, replace $ with USD, normalise to Latin-1 safe chars."""
        if not text:
            return ""
        text = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)
        text = re.sub(r"\*{2,3}([^*]+)\*{2,3}", r"\1", text)
        text = re.sub(r"\*([^*]+)\*", r"\1", text)
        text = re.sub(r"`([^`]+)`", r"\1", text)
        text = text.replace("$", "USD ")
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.translate(_UNICODE_MAP)
        # Drop any remaining non-Latin-1 characters
        text = text.encode("latin-1", errors="replace").decode("latin-1")
        return text.strip()

    def usd(value: Any) -> str:
        if value is None:
            return "N/A"
        try:
            return f"USD {float(value):,.2f}"
        except (ValueError, TypeError):
            return str(value)

    def pct(value: Any) -> str:
        if value is None:
            return "N/A"
        try:
            return f"{float(value) * 100:.1f}%"
        except (ValueError, TypeError):
            return str(value)

    # -----------------------------------------------------------------------
    # Colour palette (RGB tuples)
    # -----------------------------------------------------------------------
    NAVY  = (15,  23,  42)
    GREEN = (34,  197, 94)
    GRAY  = (100, 116, 139)
    LIGHT = (241, 245, 249)
    WHITE = (255, 255, 255)
    AMBER = (155,  100,  10)
    RED   = (185,   50,  50)
    BODY  = (30,   40,  60)
    MUTED = (150, 160, 175)

    # -----------------------------------------------------------------------
    # PDF class with footer
    # -----------------------------------------------------------------------
    class _PDF(FPDF):
        def footer(self):
            self.set_y(-13)
            self.set_font("Helvetica", "I", 7)
            self.set_text_color(*MUTED)
            self.cell(
                0, 5,
                f"Neatly AI  |  Confidential  |  Page {self.page_no()}",
                align="C",
            )

    pdf = _PDF()
    pdf.set_margins(18, 18, 18)
    pdf.set_auto_page_break(auto=True, margin=22)
    pdf.add_page()

    pw = pdf.w - 36    # printable width (A4 = 210 - 36 = 174 mm)
    lm = 18             # left margin

    # -----------------------------------------------------------------------
    # Layout helpers — always reset X to lm before text output
    # -----------------------------------------------------------------------
    def mc(h: float, text: str) -> None:
        """Multi-cell helper: always starts from left margin, full printable width."""
        pdf.set_x(lm)
        pdf.multi_cell(pw, h, text)

    def mc_indent(h: float, text: str, indent: float = 6.0) -> None:
        """Indented multi-cell helper."""
        pdf.set_x(lm + indent)
        pdf.multi_cell(pw - indent, h, text)

    def sec(title: str, gap: float = 8.0) -> None:
        """Render a section header with a green left accent bar."""
        pdf.ln(gap)
        y0 = pdf.get_y()
        pdf.set_x(lm)
        pdf.set_fill_color(*LIGHT)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*NAVY)
        pdf.cell(pw, 8, f"   {title}", fill=True, new_x="LMARGIN", new_y="NEXT")
        # Green left accent line
        pdf.set_draw_color(*GREEN)
        pdf.set_line_width(2.0)
        pdf.line(lm, y0, lm, y0 + 8)
        pdf.set_draw_color(200, 200, 200)
        pdf.set_line_width(0.2)
        pdf.ln(3)

    # -----------------------------------------------------------------------
    # Header bar
    # -----------------------------------------------------------------------
    pdf.set_fill_color(*NAVY)
    pdf.rect(0, 0, pdf.w, 42, "F")

    pdf.set_y(10)
    pdf.set_font("Helvetica", "B", 17)
    pdf.set_text_color(*WHITE)
    pdf.cell(0, 10, "Neatly AI  |  Sales Insight Report", align="C",
             new_x="LMARGIN", new_y="NEXT")

    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(180, 200, 220)
    row_count = (
        len(df) if df is not None and not df.empty
        else kpis.get("total_orders", "—")
    )
    col_count = (
        len(df.columns) if df is not None and not df.empty
        else eda.get("column_count", "")
    )
    size_str = f"{row_count} rows" + (f" x {col_count} columns" if col_count else "")
    pdf.cell(
        0, 7,
        f"{datetime.now().strftime('%B %d, %Y')}  |  {size_str}",
        align="C", new_x="LMARGIN", new_y="NEXT",
    )

    pdf.set_y(52)
    pdf.set_text_color(*NAVY)

    # -----------------------------------------------------------------------
    # KPI Table
    # -----------------------------------------------------------------------
    sec("Key Performance Indicators")

    rev_label = kpis.get("revenue_label", "Total Revenue")
    kpi_rows: list[tuple[str, str]] = []
    if "total_revenue"        in kpis: kpi_rows.append((rev_label,           usd(kpis["total_revenue"])))
    if "total_orders"         in kpis: kpi_rows.append(("Total Orders",       f"{kpis['total_orders']:,}"))
    if "average_order_value"  in kpis: kpi_rows.append(("Avg Order Value",    usd(kpis["average_order_value"])))
    if "unique_customers"     in kpis: kpi_rows.append(("Unique Customers",   f"{kpis['unique_customers']:,}"))
    if "total_units_sold"     in kpis: kpi_rows.append(("Units Sold",         f"{kpis['total_units_sold']:,.0f}"))
    if "return_rate"          in kpis: kpi_rows.append(("Return Rate",         pct(kpis["return_rate"])))
    if "churn_rate"           in kpis: kpi_rows.append(("Churn Rate",          pct(kpis["churn_rate"])))
    if "revenue_per_customer" in kpis: kpi_rows.append(("Revenue / Customer", usd(kpis["revenue_per_customer"])))

    lw = 88    # label column width (mm)
    vw = pw - lw

    for label, value in kpi_rows:
        pdf.set_x(lm)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*GRAY)
        pdf.cell(lw, 6.5, label, new_x="END", new_y="TOP")
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*NAVY)
        pdf.cell(vw, 6.5, value, new_x="LMARGIN", new_y="NEXT")

    rev_source = kpis.get("revenue_source_column", "")
    if rev_source:
        pdf.ln(1)
        pdf.set_x(lm)
        pdf.set_font("Helvetica", "I", 7.5)
        pdf.set_text_color(*GRAY)
        pdf.cell(0, 4.5, f"Revenue source: '{rev_source}' (mapped as {rev_label})",
                 new_x="LMARGIN", new_y="NEXT")

    # -----------------------------------------------------------------------
    # Revenue Trend
    # -----------------------------------------------------------------------
    rev_trend = (trends or {}).get("revenue_trend", {})
    if rev_trend and "error" not in rev_trend:
        sec("Revenue Trend")
        trend_lbl = rev_trend.get("trend_label", "unknown").upper()
        pdf.set_x(lm)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*NAVY)
        pdf.cell(pw, 6, f"Overall trend: {trend_lbl}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(1)

        for note in rev_trend.get("notes", [])[:3]:
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_text_color(*BODY)
            mc_indent(5, f"- {clean(note)}")

        ld = rev_trend.get("largest_drop")
        if ld:
            pdf.ln(1)
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_text_color(*AMBER)
            mc_indent(5, f"Largest decline: {ld['pct_change']:.1f}% in {ld['period']}")

        parts = []
        if rev_trend.get("best_period"):
            parts.append(f"Best: {rev_trend['best_period']}")
        if rev_trend.get("worst_period"):
            parts.append(f"Worst: {rev_trend['worst_period']}")
        if parts:
            pdf.set_x(lm)
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_text_color(*GRAY)
            pdf.cell(pw, 5, "  " + "  |  ".join(parts), new_x="LMARGIN", new_y="NEXT")

    # -----------------------------------------------------------------------
    # Anomalies
    # -----------------------------------------------------------------------
    if isinstance(anomalies, list):
        anom_list = anomalies
    else:
        anom_list = [a for v in (anomalies or {}).values() if isinstance(v, list) for a in v]

    if anom_list:
        sec("Anomalies Detected")
        high_count = sum(1 for a in anom_list if a.get("severity") == "high")
        pdf.set_x(lm)
        pdf.set_font("Helvetica", "", 9)
        pdf.set_text_color(*NAVY)
        pdf.cell(pw, 5.5, f"{len(anom_list)} anomalies detected, {high_count} high-severity.",
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        badge_w = 18

        for a in anom_list[:6]:
            sev       = a.get("severity", "low").upper()
            sev_col   = {"HIGH": RED, "MEDIUM": AMBER}.get(sev, GRAY)
            metric    = a.get("metric", "").replace("_", " ").title()
            period    = a.get("period", "")
            expl      = clean(a.get("explanation_ready_summary", ""))
            label_str = metric + (f"  ({period})" if period else "")

            pdf.set_x(lm)
            pdf.set_font("Helvetica", "B", 8)
            pdf.set_text_color(*sev_col)
            pdf.cell(badge_w, 5.5, f"[{sev[:3]}]", new_x="END", new_y="TOP")
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_text_color(*NAVY)
            pdf.cell(pw - badge_w, 5.5, label_str, new_x="LMARGIN", new_y="NEXT")
            if expl:
                pdf.set_font("Helvetica", "", 7.5)
                pdf.set_text_color(*GRAY)
                mc_indent(4, expl, indent=badge_w)
            pdf.ln(1)

    # -----------------------------------------------------------------------
    # Period-over-Period Drivers
    # -----------------------------------------------------------------------
    overall = (drivers or {}).get("overall_change", {})
    if overall and "error" not in overall:
        sec("Revenue Driver Analysis")
        change    = overall.get("change", 0)
        pct_val   = overall.get("change_pct")
        prev_p    = overall.get("previous_period", "")
        curr_p    = overall.get("latest_period", "")
        direction = "increase" if change >= 0 else "decline"
        delta_str = usd(abs(change)) + (f" ({pct_val:+.1f}%)" if pct_val is not None else "")

        pdf.set_font("Helvetica", "", 9.5)
        pdf.set_text_color(*NAVY)
        mc(6, f"Revenue {direction} of {delta_str} from {prev_p} to {curr_p}.")

        for dim in ("category", "product", "region", "channel"):
            dim_drivers_list = (drivers or {}).get(f"drivers_by_{dim}", [])
            if dim_drivers_list:
                top      = dim_drivers_list[0]
                dim_val  = top.get("dimension_value", "")
                dim_chg  = top.get("absolute_change", 0)
                dim_pct  = top.get("contribution_to_total_change_pct", 0)
                sign_str = "+" if dim_chg >= 0 else ""
                note     = (
                    f"Top {dim}: {dim_val}"
                    f"  ({sign_str}{dim_pct:.1f}% of total change, {usd(abs(dim_chg))})"
                )
                pdf.set_font("Helvetica", "", 8.5)
                pdf.set_text_color(*GRAY)
                mc_indent(5, note)
                break

    # Concentration risk
    conc = (static_drivers or {}).get("concentration_warning")
    if conc:
        pdf.ln(3)
        pdf.set_font("Helvetica", "I", 8.5)
        pdf.set_text_color(*AMBER)
        mc(5, f"Concentration risk: {clean(conc)}")

    # -----------------------------------------------------------------------
    # Executive Summary
    # -----------------------------------------------------------------------
    if summary:
        sec("Executive Summary")
        lines = summary.split("\n")

        for line in lines:
            stripped = line.strip()

            if not stripped:
                pdf.ln(2)
                continue

            # H1 / H2
            m2 = re.match(r"^#{1,2}\s+(.*)", stripped)
            if m2:
                heading = clean(m2.group(1))
                if heading.lower() in ("executive summary",):
                    continue   # already shown as section header
                pdf.set_font("Helvetica", "B", 10)
                pdf.set_text_color(*NAVY)
                pdf.ln(3)
                mc(6, heading)
                continue

            # H3
            m3 = re.match(r"^###\s+(.*)", stripped)
            if m3:
                heading = clean(m3.group(1))
                pdf.set_font("Helvetica", "B", 9)
                pdf.set_text_color(*NAVY)
                pdf.ln(2)
                mc(5.5, heading)
                continue

            # Bullet list item
            if stripped.startswith("- ") or stripped.startswith("* "):
                text = clean(stripped[2:])
                pdf.set_font("Helvetica", "", 8.5)
                pdf.set_text_color(*BODY)
                mc_indent(5, f"- {text}")
                continue

            # Numbered list item
            mn = re.match(r"^(\d+)\.\s+(.*)", stripped)
            if mn:
                num  = mn.group(1)
                text = clean(mn.group(2))
                pdf.set_font("Helvetica", "", 8.5)
                pdf.set_text_color(*BODY)
                mc_indent(5, f"{num}. {text}")
                continue

            # Regular paragraph
            pdf.set_font("Helvetica", "", 8.5)
            pdf.set_text_color(*BODY)
            mc(5, clean(stripped))

    return bytes(pdf.output())
