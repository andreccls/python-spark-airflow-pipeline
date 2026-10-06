# 0001 - Medallion layers on local Parquet

Status: accepted

Raw files are landed as-is; bronze keeps them as all-string Parquet plus lineage columns;
silver types, deduplicates and quarantines bad rows (`orders_rejected` with a reason);
gold holds the business aggregate (`daily_sales`). Every table is written one `dt=` directory
at a time with overwrite, so any task can be re-run for any day without duplicating data.
Parquet on the local filesystem keeps the example runnable anywhere; swapping the base path
for S3/ADLS is a config change.
