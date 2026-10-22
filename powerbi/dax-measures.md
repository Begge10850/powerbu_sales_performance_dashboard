# Recommended explicit DAX measures

Replace `Sales` below with the actual Power BI fact-table name.

```DAX
Total Sales =
SUM ( Sales[Sales] )

Total Orders =
DISTINCTCOUNT ( Sales[Order ID] )

Sales Line Items =
COUNTROWS ( Sales )

Sales per Order =
DIVIDE ( [Total Sales], [Total Orders] )

Total Customers =
DISTINCTCOUNT ( Sales[Customer Name] )

Sales per Customer =
DIVIDE ( [Total Sales], [Total Customers] )

Sales Previous Year =
CALCULATE ( [Total Sales], SAMEPERIODLASTYEAR ( 'Date'[Date] ) )

Sales YoY Change =
[Total Sales] - [Sales Previous Year]

Sales YoY % =
DIVIDE ( [Sales YoY Change], [Sales Previous Year] )
```

## Average order value by segment

`Sales per Order` responds correctly to segment filter context when Segment is on
the visual. It divides sales for the selected segment by distinct orders in that
same context.

## Formatting

- Do not add a currency symbol until the source currency is documented.
- Total Sales: whole number or one decimal in millions.
- Sales per Order: two decimals.
- YoY percentage: one decimal percentage.
- Orders, customers, and line items: whole numbers.
