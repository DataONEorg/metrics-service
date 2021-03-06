CREATE MATERIALIZED VIEW landingpage AS
  SELECT dataset_id, metrics_name, month, year, sum(metrics_value) as sum
  FROM metrics group by dataset_id, metrics_name, ROLLUP(year, month);
CREATE MATERIALIZED VIEW userprofilemetrics AS
  SELECT user_id, dataset_id, metrics_name, sum(metrics_value) as sum
  FROM metrics group by user_id, dataset_id, metrics_name;
CREATE MATERIALIZED VIEW userprofilecharts AS
  SELECT user_id, dataset_id, metrics_name, month, year, sum(metrics_value) as sum
  FROM metrics group by user_id, dataset_id, metrics_name, ROLLUP(year, month);
CREATE MATERIALIZED VIEW repometrics AS
  SELECT repository, dataset_id, metrics_name, sum(metrics_value) as sum
  FROM metrics group by repository, dataset_id, metrics_name;
CREATE MATERIALIZED VIEW repocharts AS
  SELECT repository, metrics_name, month, year, sum(metrics_value) as sum
  FROM metrics group by repository, metrics_name, ROLLUP(year, month);
CREATE MATERIALIZED VIEW awardmetrics AS
  SELECT award_number, dataset_id, metrics_name, sum(metrics_value) as sum
  FROM metrics group by award_number, dataset_id, metrics_name;
CREATE MATERIALIZED VIEW awardcharts AS
  SELECT award_number, metrics_name, month, year, sum(metrics_value) as sum
  FROM metrics group by award_number, metrics_name, ROLLUP(year, month);
