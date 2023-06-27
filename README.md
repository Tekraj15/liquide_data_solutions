# liquide_data_solutions

lambda_func_data_extract.py - Lambda Function which regularly extracts data from the API and writes to S3, with load_date partition. The Lambda function can be scheduled with Step Function.

liquide_etl.py - AWS Glue ETL job used for processing the raw data, and convert the data into parquet format for efficient data query and retrieval, into s3 folder.

The ETL job can be scheduled using the Step function and it will be triggered once the Lambda Function successfully extracts data and uploads it in S3.
