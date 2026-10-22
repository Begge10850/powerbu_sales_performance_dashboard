# What you need to do

The improvement package is prepared locally. The remaining actions require your
authenticated Databricks or Windows Power BI environment.

## 1. Databricks — create a new notebook

Do not overwrite the original yet.

1. In Databricks, choose **Workspace → Create → Import**.
2. Upload `notebooks/Superstore_BI_Notebook_Improved.py`.
3. Name it `Superstore_BI_Notebook_Improved`.
4. Confirm that the `train` table is still available.
5. Run the notebook one section at a time through the data-quality checks.
6. Stop and record any failed assertion or unexpected validation result.
7. Run the corrected analysis cells.
8. Save the corrected shipping-mode and average-order-value results.
9. Run the persistent-table publication section last. It creates
   `superstore_sales_clean_v2` in the active catalog/schema and refuses to overwrite
   a table with that name.
10. Confirm the published summary reports 9,800 rows, 4,922 orders, and
    2,261,536.78 in total sales.

## 2. Send back the validation evidence

Provide these outputs for final verification:

- the printed validation dictionary;
- corrected shipping-mode result;
- corrected average-order-value result;
- published-table name and reconciliation summary.

Do not publish corrected numerical claims until these cells have run successfully.

## 3. Power BI Desktop — use Windows

1. Make a copy of `Sales_report.pbix`.
2. Connect the copied report to the published Databricks table through its SQL
   warehouse, preferably in Import mode for the portfolio version.
3. Refresh and resolve any column-path changes.
4. Add a dedicated Date table.
5. add the measures from `powerbi/dax-measures.md`.
6. Apply the visual changes in `powerbi/dashboard-improvement-guide.md`.
7. Confirm the unfiltered KPI targets:
   - 9,800 line items;
   - 4,922 orders;
   - 2,261,536.78 total sales;
   - 459.48 sales per order.
8. Test both slicers and all cross-filter interactions.
9. Save the revised report under a new name.
10. Export two report-only 16:9 screenshots without Power BI navigation chrome.

## 4. Review before GitHub publication

Compare the local revised files with the current repository. After approving them:

- replace the README;
- add the improved notebook under `notebooks/`;
- replace the truncated CSV with the complete export;
- add the documentation and clean screenshots;
- retain the original reports only if clearly labelled, or replace them with revised
  PDFs generated from the corrected report text;
- keep the Power BI link labelled `sign-in required`;
- commit with a message such as `Improve validation, metrics, and documentation`.

Nothing in the GitHub repository, live notebook, Power BI service, or portfolio site
has been changed by this local preparation.
