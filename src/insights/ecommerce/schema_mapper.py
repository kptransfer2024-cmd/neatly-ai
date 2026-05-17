"""Rule-based e-commerce column inference — no LLM calls, fully deterministic.

Inference uses three signals in priority order:
  1. exact_alias   — normalized column name exactly matches a known alias
  2. fuzzy_alias   — normalized column name contains or is contained by an alias
  3. dtype_inference — column dtype / value distribution matches expected role

All column names are normalized before matching:
  lower-cased, stripped, spaces/hyphens/punctuation replaced with underscores.

Returns backward-compatible mapping dict PLUS a 'mapping_details' key with
per-field confidence scores, reasons, and match types.
"""
from __future__ import annotations

import re
import pandas as pd

# ---------------------------------------------------------------------------
# Alias tables — ordered by priority (first alias = primary / most canonical)
# ---------------------------------------------------------------------------

_ALIASES: dict[str, list[str]] = {
    "date": [
        "date", "order_date", "purchase_date", "transaction_date", "created_at",
        "invoice_date", "sale_date", "sales_date", "timestamp", "time", "datetime",
    ],
    "revenue": [
        "revenue", "sales", "total_sales", "amount", "total_amount", "order_value",
        "transaction_value", "transaction_amount", "purchase_amount", "payment_amount",
        "price_total", "subtotal", "net_sales", "gross_sales", "sales_amount",
        "item_total", "invoice_amount",
    ],
    "order_id": [
        "order_id", "transaction_id", "invoice_id", "receipt_id", "purchase_id", "sale_id",
    ],
    "customer_id": [
        "customer_id", "user_id", "buyer_id", "client_id", "member_id", "account_id",
    ],
    "customer_name": [
        "customer_name", "name", "buyer_name", "client_name",
    ],
    "product": [
        "product", "product_name", "item", "item_name", "sku",
    ],
    "category": [
        "category", "product_category", "department", "segment",
    ],
    "quantity": [
        "quantity", "qty", "units", "units_sold", "items_sold", "count",
    ],
    "price": [
        "price", "unit_price", "product_price", "item_price", "list_price",
    ],
    "discount": [
        "discount", "discount_amount", "coupon", "promo", "markdown",
    ],
    "return_flag": [
        "return", "returned", "return_status", "is_returned", "refund",
        "refunded", "refund_status",
    ],
    "churn_flag": [
        "churn", "churned", "is_churned", "customer_churn", "churn_status",
    ],
    "region": [
        "region", "state", "country", "market", "city",
    ],
    "channel": [
        "channel", "source", "platform",
    ],
    "payment_method": [
        "payment_method", "payment_type", "method",
    ],
}

_RECOMMENDED = {"date", "revenue", "order_id", "customer_id"}

# Keywords that indicate monetary/revenue-like columns (for dtype inference)
_REVENUE_KEYWORDS = frozenset({
    "value", "amount", "sales", "revenue", "total", "price",
    "transaction", "purchase", "payment", "subtotal", "net", "gross", "invoice",
})
# Keywords that indicate quantity-like columns
_QTY_KEYWORDS = frozenset({"quantity", "qty", "units", "count", "items", "sold"})


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _normalize(name: str) -> str:
    """Lower-case, strip, replace spaces/punctuation with underscores, collapse runs."""
    name = name.lower().strip()
    name = re.sub(r"[\s\-\/\.,;:()\[\]{}\'\"]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


# ---------------------------------------------------------------------------
# Alias scoring
# ---------------------------------------------------------------------------

def _alias_score(normalized_col: str, aliases: list[str]) -> tuple[float, str, str]:
    """Return (score, match_type, matched_alias) for the best alias match.

    score=0 means no match found.
    """
    # Exact match on normalized alias
    for i, alias in enumerate(aliases):
        norm_alias = _normalize(alias)
        if normalized_col == norm_alias:
            conf = 0.97 if i == 0 else 0.88
            return conf, "exact_alias", alias

    # Fuzzy: normalized col fully contains alias or alias fully contains col
    for i, alias in enumerate(aliases):
        norm_alias = _normalize(alias)
        if len(norm_alias) < 3:
            continue  # skip very short aliases to avoid false positives
        if norm_alias in normalized_col or normalized_col in norm_alias:
            conf = 0.74 if i == 0 else 0.66
            return conf, "fuzzy_alias", alias

    return 0.0, "", ""


# ---------------------------------------------------------------------------
# Dtype-based inference helpers
# ---------------------------------------------------------------------------

def _infer_date_confidence(col: str, df: pd.DataFrame) -> float:
    """Return confidence (0–1) that column contains date/time values."""
    series = df[col]
    if pd.api.types.is_datetime64_any_dtype(series):
        return 0.92
    if series.dtype in ("object", "str", "string"):
        sample = series.dropna().head(50)
        if len(sample) == 0:
            return 0.0
        try:
            parsed = pd.to_datetime(sample, errors="coerce", infer_datetime_format=True)
            parse_rate = float(parsed.notna().mean())
            if parse_rate >= 0.85:
                return 0.78
            if parse_rate >= 0.60:
                return 0.58
        except Exception:
            pass
    return 0.0


def _infer_revenue_confidence(col: str, df: pd.DataFrame) -> float:
    """Return confidence (0–1) that column is a monetary/revenue metric."""
    series = pd.to_numeric(df[col], errors="coerce")
    valid = series.dropna()
    if len(valid) == 0:
        return 0.0
    pos_rate = float((valid > 0).mean())
    if pos_rate < 0.5:
        return 0.0
    norm_col = _normalize(col)
    name_hits = sum(1 for kw in _REVENUE_KEYWORDS if kw in norm_col)
    has_decimal = bool((valid % 1 != 0).any())
    mean_val = float(valid.mean())
    if mean_val < 1.0:
        return 0.0
    if name_hits >= 2:
        return 0.70
    if name_hits >= 1 and mean_val > 5:
        return 0.63
    if has_decimal and mean_val > 10:
        return 0.55
    return 0.0


def _infer_quantity_confidence(col: str, df: pd.DataFrame) -> float:
    """Return confidence (0–1) that column is a unit quantity metric."""
    series = pd.to_numeric(df[col], errors="coerce")
    valid = series.dropna()
    if len(valid) == 0:
        return 0.0
    all_int = bool((valid % 1 == 0).all())
    pos_rate = float((valid >= 0).mean())
    max_val = float(valid.max())
    norm_col = _normalize(col)
    name_hits = sum(1 for kw in _QTY_KEYWORDS if kw in norm_col)
    if all_int and pos_rate > 0.9 and max_val < 1000 and name_hits >= 1:
        return 0.72
    if all_int and pos_rate > 0.9 and max_val < 200:
        return 0.55
    return 0.0


def _infer_boolean_confidence(col: str, df: pd.DataFrame) -> float:
    """Return confidence (0–1) that column is a binary/categorical flag."""
    series = df[col]
    if series.dtype == "bool":
        return 0.95
    normalized = series.astype(str).str.lower().str.strip()
    unique_vals = set(normalized.dropna().unique()) - {"nan", "none", ""}
    _BOOL_SETS = [
        {"true", "false"}, {"yes", "no"}, {"1", "0"},
        {"returned", "not returned"}, {"returned", "not_returned"},
        {"churned", "not churned"}, {"churned", "active"},
        {"y", "n"},
    ]
    for bset in _BOOL_SETS:
        if unique_vals and unique_vals.issubset(bset):
            return 0.88
    # Allow up to 3 unique non-null values (some mixed formats)
    if 0 < len(unique_vals) <= 3:
        return 0.62
    return 0.0


# ---------------------------------------------------------------------------
# Main inference entry point
# ---------------------------------------------------------------------------

def infer_ecommerce_schema(df: pd.DataFrame) -> dict:
    """Map DataFrame columns to e-commerce semantic fields using multi-signal inference.

    Returns a dict with:
      - <field>: mapped column name (or None) for all fields in _ALIASES
      - confidence: dict[field, 'high'|'medium'|'low']
      - missing_recommended_fields: list of undetected recommended fields
      - mapping_details: dict[field, {column, confidence, reason, match_type, alternatives}]
    """
    if df.empty:
        mapping: dict = {field: None for field in _ALIASES}
        mapping["confidence"] = {f: "low" for f in _ALIASES}
        mapping["missing_recommended_fields"] = list(_RECOMMENDED)
        mapping["mapping_details"] = {}
        return mapping

    # Build normalized → original column lookup
    norm_to_orig: dict[str, str] = {_normalize(c): c for c in df.columns}

    mapping_out: dict[str, str | None] = {field: None for field in _ALIASES}
    mapping_details: dict[str, dict] = {}
    confidence: dict[str, str] = {}
    claimed: set[str] = set()  # original column names already assigned

    # -----------------------------------------------------------------------
    # Pass 1: Alias matching (exact then fuzzy) across all fields
    # -----------------------------------------------------------------------
    for field, aliases in _ALIASES.items():
        best_score = 0.0
        best_orig: str | None = None
        best_type = ""
        best_alias = ""
        alternatives: list[str] = []

        for norm_col, orig_col in norm_to_orig.items():
            score, mtype, matched_alias = _alias_score(norm_col, aliases)
            if score > best_score:
                if best_orig is not None:
                    alternatives.append(best_orig)
                best_score = score
                best_orig = orig_col
                best_type = mtype
                best_alias = matched_alias
            elif score > 0.5 and orig_col != best_orig:
                alternatives.append(orig_col)

        if best_score > 0.5 and best_orig is not None:
            mapping_out[field] = best_orig
            confidence[field] = _score_to_qualitative(best_score)
            mapping_details[field] = {
                "column": best_orig,
                "confidence": round(best_score, 3),
                "reason": f"Matched alias '{best_alias}' via {best_type.replace('_', ' ')}.",
                "match_type": best_type,
                "alternatives": [a for a in alternatives if a != best_orig][:3],
            }
            claimed.add(best_orig)

    # -----------------------------------------------------------------------
    # Pass 2: Dtype inference for fields still unmapped
    # -----------------------------------------------------------------------
    unclaimed = [c for c in df.columns if c not in claimed]

    # --- Date ---
    if mapping_out.get("date") is None:
        best_score, best_col = 0.0, None
        for col in unclaimed:
            s = _infer_date_confidence(col, df)
            if s > best_score:
                best_score, best_col = s, col
        if best_col and best_score >= 0.58:
            mapping_out["date"] = best_col
            confidence["date"] = _score_to_qualitative(best_score)
            mapping_details["date"] = {
                "column": best_col,
                "confidence": round(best_score, 3),
                "reason": f"'{best_col}' parses as datetime values.",
                "match_type": "dtype_inference",
                "alternatives": [],
            }
            claimed.add(best_col)
            unclaimed = [c for c in unclaimed if c != best_col]

    # --- Revenue ---
    if mapping_out.get("revenue") is None:
        best_score, best_col = 0.0, None
        for col in unclaimed:
            s = _infer_revenue_confidence(col, df)
            if s > best_score:
                best_score, best_col = s, col
        if best_col and best_score >= 0.55:
            mapping_out["revenue"] = best_col
            confidence["revenue"] = _score_to_qualitative(best_score)
            mapping_details["revenue"] = {
                "column": best_col,
                "confidence": round(best_score, 3),
                "reason": f"'{best_col}' is numeric with monetary name keywords and positive distribution.",
                "match_type": "dtype_inference",
                "alternatives": [],
            }
            claimed.add(best_col)
            unclaimed = [c for c in unclaimed if c != best_col]

    # --- Quantity ---
    if mapping_out.get("quantity") is None:
        best_score, best_col = 0.0, None
        for col in unclaimed:
            s = _infer_quantity_confidence(col, df)
            if s > best_score:
                best_score, best_col = s, col
        if best_col and best_score >= 0.55:
            mapping_out["quantity"] = best_col
            confidence["quantity"] = _score_to_qualitative(best_score)
            mapping_details["quantity"] = {
                "column": best_col,
                "confidence": round(best_score, 3),
                "reason": f"'{best_col}' contains non-negative integers consistent with unit counts.",
                "match_type": "dtype_inference",
                "alternatives": [],
            }
            claimed.add(best_col)
            unclaimed = [c for c in unclaimed if c != best_col]

    # --- Return flag ---
    if mapping_out.get("return_flag") is None:
        _try_binary_field("return_flag", ["return", "refund"], unclaimed, df, mapping_out, confidence, mapping_details, claimed)
        unclaimed = [c for c in unclaimed if c not in claimed]

    # --- Churn flag ---
    if mapping_out.get("churn_flag") is None:
        _try_binary_field("churn_flag", ["churn"], unclaimed, df, mapping_out, confidence, mapping_details, claimed)
        unclaimed = [c for c in unclaimed if c not in claimed]

    # Fill low confidence for anything still unmapped
    for field in _ALIASES:
        if field not in confidence:
            confidence[field] = "low"

    missing_recommended = [f for f in _RECOMMENDED if mapping_out[f] is None]

    return {
        **mapping_out,
        "confidence": confidence,
        "missing_recommended_fields": missing_recommended,
        "mapping_details": mapping_details,
    }


def _try_binary_field(
    field: str,
    name_keywords: list[str],
    unclaimed: list[str],
    df: pd.DataFrame,
    mapping_out: dict,
    confidence: dict,
    mapping_details: dict,
    claimed: set,
) -> None:
    """Attempt to map a binary flag field by name keyword + boolean distribution."""
    for col in unclaimed:
        norm_col = _normalize(col)
        if any(kw in norm_col for kw in name_keywords):
            score = _infer_boolean_confidence(col, df)
            if score >= 0.55:
                mapping_out[field] = col
                confidence[field] = _score_to_qualitative(score)
                mapping_details[field] = {
                    "column": col,
                    "confidence": round(score, 3),
                    "reason": f"'{col}' appears binary/categorical with name matching {name_keywords}.",
                    "match_type": "dtype_inference",
                    "alternatives": [],
                }
                claimed.add(col)
                return


def _score_to_qualitative(score: float) -> str:
    """Convert numeric confidence score to qualitative label."""
    if score >= 0.85:
        return "high"
    if score >= 0.65:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_schema_mapping(mapping: dict, df: pd.DataFrame) -> dict:
    """Validate that mapped columns exist in df and have usable dtypes."""
    _SKIP = {"confidence", "missing_recommended_fields", "mapping_details"}
    issues: list[str] = []
    valid: dict[str, bool] = {}

    for field, col in mapping.items():
        if field in _SKIP:
            continue
        if col is None:
            valid[field] = False
            continue
        if col not in df.columns:
            issues.append(f"Mapped column '{col}' for field '{field}' not found in DataFrame.")
            valid[field] = False
            continue
        valid[field] = True

    return {"valid": valid, "issues": issues, "usable": len(issues) == 0}
