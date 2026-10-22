# Technical report — revised evidence-based version

## Objective

This independent portfolio project demonstrates an end-to-end business-intelligence
workflow for analysing US retail sales between 2015 and 2018. The implementation
uses Databricks, PySpark, Spark SQL, and Power BI.

## Data preparation

The registered Databricks `train` table contains 9,800 product-level sales lines.
The workflow validates the expected schema, converts date and sales fields, and
removes presentation fields that are not required for the current analysis. Column
removal does not remove rows, and the revised notebook asserts source-to-prepared
row-count equality.

Nulls, exact-row duplicates, non-positive sales, invalid shipping dates, date range,
distinct orders, and total sales are reported explicitly. Exact duplicates are not
automatically removed because repeated records require business interpretation and
repeated Order IDs normally represent legitimate product lines.

## Metric design

The revised workflow distinguishes three analytical grains:

- line item: an individual product row;
- order: a distinct Order ID;
- customer: a stable customer entity, preferably represented by Customer ID.

Orders and shipping-mode usage use distinct Order IDs. Average order value is
calculated only after aggregating product lines to order totals.

## Analysis

Spark SQL evaluates headline KPIs, regional and product sales, chronological monthly
trends, leading customers and states, distinct-order shipping usage, order-level
average value by segment, and monthly segment performance.

The reviewed original source records 2,261,536.78 in sales across 4,922 orders,
equivalent to 459.48 per order. Consumer contributes approximately 50.8% of sales;
West and East together contribute approximately 61.0%; Technology leads categories;
and Q4 accounts for approximately 38.5%.

## Export and Power BI

The original GitHub CSV contains 9,708 rows because it matches a Databricks result
display labelled `9,708+ rows — Truncated data`. The revised workflow writes the
Spark dataframe programmatically and asserts that the exported count equals the
prepared count before it is used by Power BI.

Power BI contains an Executive Sales Summary and Market Performance Overview. The
recommended semantic-model revision uses explicit measures for sales, distinct
orders, line items, customers, and time comparisons, supported by a dedicated Date
table.

## Limitations

The available data does not contain Profit, Cost, Margin, Campaign Spend,
Conversion, Inventory, or Returns. The project therefore describes sales patterns;
it cannot establish profitability, marketing ROI, campaign effectiveness, or causal
impact. The Power BI link requires authentication and is not an anonymous public
deployment.

## Validation status

The original code, saved outputs, `.pbix` structure, screenshots, and reports have
been reviewed. The improved notebook implementation is prepared but its new
validation cells, corrected order-level results, complete export, and refreshed
Power BI KPIs remain pending execution.
