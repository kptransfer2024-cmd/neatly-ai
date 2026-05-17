# Neatly — AI E-Commerce Data Quality & Insight Copilot

Upload a messy sales dataset, clean it interactively, then get deterministic KPI analysis, trend detection, anomaly alerts, driver decomposition, and an executive summary — all privacy-safe, all exportable.

---

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: set API key for AI executive summary
export ANTHROPIC_API_KEY="sk-ant-..."

# Run the app
streamlit run src/app.py
```

Open [http://localhost:8501](http://localhost:8501) in your browser.

> `ANTHROPIC_API_KEY` is **not required**. If absent, the executive summary uses a deterministic fallback template. All KPI metrics, charts, and anomaly detection are always fully functional.

---

## Test with Sample Data

A realistic 900-row messy e-commerce CSV is included:

```
src/sample_data/messy_ecommerce_sales.csv
```

It contains:
- Inconsistent category casing (`furniture`, `Furniture`, `FURNITURE`)
- Missing values in `quantity`, `region`, `channel`
- Duplicate rows (~2% rate)
- Whitespace in product names
- Numeric revenue outliers
- A visible revenue drop in August–September (Furniture category decline)
- 12-month date span (2024)

Upload it on the first screen to get an immediate end-to-end demo.

---

## Workflow

```
Upload → Diagnose → Review & Fix → Insights → Export
```

1. **Upload** — CSV, TSV, JSON, Excel, Parquet, or direct database connection
2. **Diagnose** — 16 detectors scan for missing values, duplicates, outliers, PII, format issues, and more
3. **Review & Fix** — One-click deterministic transforms with full undo and audit log
4. **Insights** — Automatic KPI analysis, trend detection, anomaly alerts, driver decomposition, executive summary
5. **Export** — Download cleaned CSV + cleaning log JSON + insight report JSON

---

## Architecture

```
src/
├── app.py                        # Streamlit UI — stage router
├── orchestrator.py               # Diagnosis pipeline + insight pipeline
├── explanation_layer.py          # Static template explanations (zero API calls)
├── transformation_executor.py    # Deterministic pandas transforms
├── context_interpreter.py        # Column role/domain inference
│
├── detectors/                    # 16 independent issue detectors
│   ├── missing_value_detector.py
│   ├── duplicate_detector.py
│   ├── outlier_detector.py
│   ├── pii_detector.py
│   └── ...                       # (13 more)
│
├── insights/
│   └── ecommerce/
│       ├── schema_mapper.py      # Rule-based column → semantic field mapping
│       ├── kpi_calculator.py     # Revenue, orders, AOV, units, return rate, top-N tables
│       ├── eda_analyzer.py       # Dataset-level EDA summaries
│       ├── trend_analyzer.py     # Period revenue trends + labels (improving/declining/flat/volatile)
│       ├── anomaly_detector.py   # Z-score, IQR, pct-change anomalies
│       ├── driver_analyzer.py    # Revenue change decomposition by category/product/region/channel
│       └── insight_reporter.py   # Executive summary builder with privacy guard
│
├── utils/
│   ├── privacy_guards.py         # Validates no raw rows in any external payload
│   ├── formatting.py             # Currency/pct/number formatters for the UI
│   └── ...                       # (8 existing utils)
│
├── sample_data/
│   └── messy_ecommerce_sales.csv
│
└── tests/                        # 828 tests
```

### Data Flow

```
User uploads CSV
      ↓
orchestrator.run_diagnosis(df)
  → 16 detectors → issues[]
  → explanation_layer.explain_issues()
  → context_interpreter.build_column_contexts()
  → quality_score
      ↓
User reviews and fixes issues (transformation_executor)
      ↓
orchestrator.build_executive_report(df)
  → insights/ecommerce/schema_mapper   → semantic field mapping
  → insights/ecommerce/kpi_calculator  → KPI aggregates
  → insights/ecommerce/eda_analyzer    → dataset summary
  → insights/ecommerce/trend_analyzer  → period trends
  → insights/ecommerce/anomaly_detector → anomalies
  → insights/ecommerce/driver_analyzer → driver decomposition
  → insights/ecommerce/insight_reporter → executive summary
      ↓
Export: cleaned_data.csv + cleaning_log.json + insight_report.json
```

---

## Privacy Guardrails

**Hard rule: No raw DataFrame rows are ever sent to any LLM or external service.**

This is enforced in two places:

### 1. `utils/privacy_guards.py`
Called before any external API invocation:
```python
from utils.privacy_guards import validate_no_raw_rows
validate_no_raw_rows(payload, context="my_operation")
# Raises ValueError if payload contains: row_indices, raw_rows, sample_rows,
# individual_records, or any list of >500 dicts (likely raw records)
```

### 2. `insights/ecommerce/insight_reporter.py`
`validate_no_raw_rows_in_insight_payload(payload)` is called automatically inside `build_insight_payload()` and before every `generate_executive_summary()` API call. The AI always receives only aggregate summaries (totals, means, top-N lists, trend labels).

### What the AI receives
```json
{
  "kpis": { "total_revenue": 127450.0, "total_orders": 892, "average_order_value": 142.9 },
  "trends": { "revenue_trend": { "trend_label": "declining", "latest_period": "2024-09" } },
  "anomalies": [{ "metric": "revenue", "period": "2024-08", "severity": "high", "explanation_ready_summary": "..." }],
  "drivers": { "overall_change": { "change": -12400.0, "change_pct": -9.7 } }
}
```

### What the AI never receives
- Individual row values
- Customer IDs or names
- Raw transaction records
- Any column with cardinality > threshold treated as row-level data

---

## What the AI Does and Does Not Do

| Task | Handled by | LLM involved? |
|------|-----------|--------------|
| Detect data quality issues | Python/pandas detectors | No |
| Explain issues in plain English | Static templates | No |
| Apply data transforms | Deterministic pandas | No |
| Calculate KPIs (revenue, AOV, etc.) | pandas aggregations | No |
| Detect trends and anomalies | Statistical methods (z-score, IQR) | No |
| Decompose revenue drivers | pandas groupby + arithmetic | No |
| Write executive narrative | Claude API (optional) | Yes — aggregate data only |

---

## How to Run Tests

```bash
# From src/ directory
cd src
pytest                          # All 828 tests
pytest tests/ -v                # Verbose
pytest tests/test_kpi_calculator.py -v    # Specific module
pytest tests/ -k "privacy"     # Filter by name
pytest tests/ --tb=short -q    # Summary mode
```

---

## Limitations and Future Roadmap

**Current limitations:**
- Schema mapping is rule-based (column name aliases only); unusual column names may not map correctly
- Trend analysis requires at least 4 time periods for meaningful output
- Anomaly detection works best with ≥12 months of data
- Driver analysis compares only the two most recent completed periods
- Executive summary quality depends on `ANTHROPIC_API_KEY` availability

**Roadmap:**
- Interactive column re-mapping UI when auto-detection confidence is low
- Cohort analysis (new vs. returning customers)
- Category forecasting (next-period revenue projection)
- Slack / email summary delivery
- Multi-dataset comparison (month-over-month vs. year-over-year toggle)
- PDF executive report export

---

## Admin Dashboard

```bash
streamlit run src/admin_app.py --server.port 8502
```

Shows session activity, detector fire rates, and user action patterns. Requires Supabase or local JSONL logs.

---

© 2026 Kunpeng Liu · [liukunpeng267@gmail.com](mailto:liukunpeng267@gmail.com)
