# Sales Performance Analytics — Databricks and Power BI

An end-to-end business-intelligence portfolio project analysing US retail sales
from 2015–2018 with Databricks, PySpark, Spark SQL, and Power BI.

> Portfolio project, not a commercial deployment. The analysis is descriptive and
> does not claim production adoption or measured business impact.

## Project objective

The project answers how recorded sales vary by time, customer segment, region,
state, category, sub-category, customer, and shipping mode. The technical workflow
prepares and validates the data in Databricks; Power BI presents the results through
an executive summary and an interactive market breakdown.

## Architecture

```text
Registered Databricks `train` table
              ↓
PySpark schema and quality validation
              ↓
Temporary Spark SQL analytical view
              ↓
Business queries and reconciled KPIs
              ↓
Validated persistent Delta table
              ↓
Power BI semantic model and two report pages
              ↓
Technical and stakeholder communication
```

## Verified project scope

| Item | Evidence-backed value |
|---|---:|
| Sales line items | 9,800 |
| Distinct orders | 4,922 |
| Recorded sales value | 2,261,536.78 |
| Sales per order | 459.48 |
| Period | 2015–2018 |
| Geography | United States |

The source currency is not documented, so this README deliberately uses
“recorded sales value” rather than assigning a currency.

## Selected findings

- Consumer accounted for approximately 50.8% of recorded sales.
- West and East together contributed approximately 61.0%.
- Technology was the leading category at approximately 827.5K.
- Q4 accounted for approximately 38.5% of recorded sales.
- California led states and Sean Miller led customers by recorded sales.
- Annual sales fell in 2016, then increased in 2017 and 2018; the data does not
  support a claim of uninterrupted year-over-year growth.

These are descriptive findings. They motivate further investigation but do not
prove profitability, marketing effectiveness, or causality.

## Dashboard pages

### Executive Sales Summary

Headline KPIs with segment, regional, and annual sales views.

### Market Performance Overview

Interactive year and segment filtering with category, sub-category, shipping,
and monthly breakdowns.

## Corrections in the improved version

- Distinguishes sales line items from distinct orders.
- Calculates average order value after aggregating product lines to order level.
- Counts distinct orders for shipping-mode usage.
- Adds row-count, schema, null, duplicate, date, and sales validation.
- Preserves chronological year-month values.
- Publishes a validated persistent Delta table for direct Power BI connectivity
  rather than downloading a truncated Databricks display.
- Replaces unsupported profitability, ROI, and conversion language with
  evidence-based sales terminology.

## Repository contents

| Path | Purpose |
|---|---|
| `notebooks/Superstore_BI_Notebook_Improved.py` | Databricks-importable validated notebook |
| `documentation/metric-definitions.md` | Business definitions and calculation rules |
| `documentation/data-dictionary.md` | Field descriptions and modelling notes |
| `documentation/validation-plan.md` | Acceptance criteria and retained evidence |
| `powerbi/dax-measures.md` | Explicit Power BI measure definitions |
| `powerbi/dashboard-improvement-guide.md` | Visual and model changes to apply in Desktop |
| `reports/technical-report-revised.md` | Corrected technical narrative |
| `reports/stakeholder-report-revised.md` | Claim-safe business narrative |
| `website/portfolio-project-copy.md` | Website-ready case-study copy |

## Reproduction

1. Import `notebooks/Superstore_BI_Notebook_Improved.py` into Databricks.
2. Confirm that the registered source table is named `train`.
3. Run cells through the validation section and inspect failures before continuing.
4. Run the analysis cells.
5. Run the publication cell and confirm the persistent table contains 9,800 rows,
   4,922 orders, and 2,261,536.78 in total sales.
6. Connect Power BI to the Databricks SQL warehouse and select the published table.
7. Refresh a copy of the report.
8. Apply the explicit DAX measures and verify the KPI reconciliation table.

## Known limitations

- The data contains Sales but not Profit, Cost, Margin, Discount, Campaign Spend,
  Conversion, Inventory, or Returns.
- Exact duplicate rows must be investigated rather than removed automatically;
  repeated Order IDs normally represent legitimate product lines.
- The Power BI share link requires Microsoft authentication and is not a public
  anonymous portfolio link.
- The existing GitHub CSV contains only 9,708 rows because it matches the boundary
  of a Databricks result labelled “9,708+ rows — Truncated data.”
- Recommendations are hypotheses for future validation, not measured outcomes.

## Tools demonstrated

Databricks · PySpark · Spark SQL · data-quality validation · Power BI · DAX ·
dashboard design · business reporting · Git/GitHub

## Links

- [GitHub repository](https://github.com/Begge10850/powerbu_sales_performance_dashboard)
- [Dashboard demonstration](https://youtu.be/w2pjmoucwtY)
- Databricks and Power BI workspace links require authentication.

## Author

Treva Antony Ogwang — [LinkedIn](https://www.linkedin.com/in/treva-ogwang-87235626b/) ·
[GitHub](https://github.com/Begge10850)
