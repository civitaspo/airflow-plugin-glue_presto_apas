CREATE SCHEMA example_db;
CREATE EXTERNAL TABLE `example_table`(
  `column_name` string)
PARTITIONED BY (
  `table_schema` string,
  `table_name` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://path/to/example_db/example_table';