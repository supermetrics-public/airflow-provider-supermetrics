# Changelog

## 0.1.0 (2026-04-08)

Initial release of the Supermetrics Airflow provider.

### Added

- `SupermetricsHook` with connection UI (API key in Password field)
- `SupermetricsQueryOperator` for ad-hoc queries with XCom integration
- `SupermetricsQueryToGCSOperator` for streaming query results to GCS
- `SupermetricsQueryToS3Operator` for streaming query results to S3
- `SupermetricsCreateBackfillOperator` with deferrable mode support
- `SupermetricsCancelBackfillOperator` for backfill cleanup
- `SupermetricsBackfillSensor` for poke-based backfill monitoring
- `SupermetricsBackfillTrigger` for async backfill monitoring via Triggerer
- `SupermetricsCreateLoginLinkOperator` for OAuth login links
- `SupermetricsListLoginsOperator` for listing logins
- `SupermetricsGetLoginOperator` for retrieving login details
- Example DAGs for query, backfill, GCS, and S3 use cases
