# liquide_data_solutions

lambda_func_data_extract.py - Lambda Function which regularly extracts data from the API and writes to S3, with load_date partition. The Lambda function can be scheduled with Step Function.

liquide_etl.py - AWS Glue ETL job used for processing the raw data, and convert the data into parquet format for efficient data query and retrieval, into s3 folder.

The ETL job can be scheduled either using Step function which will be triggerred once the data extracted by lambda function will be uploaded in S3.
