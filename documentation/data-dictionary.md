# Data dictionary

| Field | Role | Notes |
|---|---|---|
| Order ID | Degenerate dimension / order key | Use distinct count for orders |
| Order Date | Date | Drives year, quarter, and month analysis |
| Ship Date | Date | Must not precede Order Date |
| Ship Mode | Dimension | Analyse usage at distinct-order grain |
| Customer Name | Dimension | Display label; a stable Customer ID is preferable for modelling |
| Segment | Dimension | Consumer, Corporate, Home Office |
| Country | Dimension | Current dataset contains United States only |
| City | Geography | Combine with State to reduce ambiguity |
| State | Geography | 49 states/territories represented in reviewed output |
| Region | Geography | Central, East, South, West |
| Category | Product dimension | Furniture, Office Supplies, Technology |
| Sub-Category | Product dimension | 17 reviewed values |
| Product Name | Product dimension | Display description; stable Product ID is preferable |
| Sales | Additive measure | Currency/source unit must be documented before adding a symbol |

## Fields removed from the presentation export

`Row ID` and `Postal Code` are unnecessary for the current visuals. Customer ID
and Product ID were removed in the original notebook, but retaining them in a
future semantic model is recommended because names can change or collide.

## Published Power BI table naming

The persistent Delta table uses portable snake_case field names because default
Delta column-name rules reject spaces and punctuation. Examples include `order_id`,
`order_date`, `ship_mode`, `customer_name`, `sub_category`, `product_name`,
`order_year`, and `order_month`.
