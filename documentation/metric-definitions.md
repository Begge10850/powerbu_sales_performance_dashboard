# Metric definitions

| Metric | Definition | Correct grain |
|---|---|---|
| Sales line items | Number of rows in the prepared sales fact data | Product line |
| Orders | Distinct count of `Order ID` | Order |
| Total sales | Sum of `Sales` | Line values aggregated to selection |
| Sales per order | Total sales divided by distinct orders | Selection |
| Average order value by segment | Average of order totals after grouping by Segment and Order ID | Order |
| Customers | Distinct customer identifier; use Customer Name only when no stable ID is available | Customer |
| Shipping-mode usage | Distinct orders grouped by Ship Mode | Order |
| Monthly sales | Sum of Sales grouped by `yyyy-MM` Order Date | Calendar month |
| Category/sub-category sales | Sum of Sales grouped by the relevant product field | Product grouping |

## Language rules

Use “sales,” “recorded sales,” or “sales value.” Do not use “profit,” “margin,”
“ROI,” or “conversion” unless the necessary source fields are added and validated.

An association or concentration is not causal evidence. Recommendations based on
descriptive results should be labelled hypotheses or suggested follow-up tests.
