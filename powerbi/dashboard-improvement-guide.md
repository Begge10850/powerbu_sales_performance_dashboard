# Power BI dashboard improvement guide

The existing navy-and-white visual system is suitable and does not need a complete
redesign. Preserve the two-page executive/detail structure.

## Data model

1. Replace the 9,708-row input with the validated complete 9,800-row export.
2. Create a Date table covering the minimum and maximum Order Date.
3. Mark it as the model's Date table and relate `Date[Date]` one-to-many to the
   sales fact's Order Date.
4. Hide technical key fields from the report view.
5. Add the explicit measures in `dax-measures.md`.
6. Add a temporary reconciliation table showing Line Items, Orders, Total Sales,
   and Sales per Order; confirm the expected KPI targets before publishing.

## Executive Sales Summary

- Retain the three headline cards, but use explicit measures.
- Label the cards `Recorded Sales`, `Distinct Orders`, and `Sales per Order`.
- Add a subtitle: `US retail sales | 2015–2018`.
- Keep segment and region comparisons as sorted horizontal bars.
- Use a continuous Date axis for annual/monthly trends.
- Add one concise annotation: Consumer leads sales contribution; West leads regions.

## Market Performance Overview

- Retain Year and Segment slicers.
- Replace the category donut with a sorted bar chart for easier comparison.
- Rename shipping visuals to indicate distinct orders, not line items.
- Use a continuous `yyyy-MM` axis for the monthly trend.
- Keep the category/sub-category matrix but ensure totals use explicit measures.
- Add a reset-filters button only if it can be tested reliably.

## Accessibility and presentation

- Check colour contrast and do not encode meaning with colour alone.
- Add alt text to every decision-relevant visual.
- Use consistent title case and number formats.
- Keep labels large enough for a portfolio screenshot.
- Export report-only screenshots without Power BI Service navigation chrome.
- Add a short note: `Descriptive analysis; profit and campaign data unavailable.`

## Publication check

- Verify both pages at 100% zoom.
- Test slicer interactions and visual cross-filtering.
- Confirm all KPIs reconcile under the unfiltered state.
- Capture clean 16:9 PNGs.
- Keep the current authenticated link labelled `Power BI Service (sign-in required)`.
