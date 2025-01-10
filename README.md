#Glue-Athena-QuickSight Data Analytics Pipeline
Project Diagram
![stockmarket drawio](https://github.com/user-attachments/assets/f55c6628-a598-4bf6-b562-2e8eec4bb42b)

Project Overview
This project demonstrates a complete data analytics pipeline leveraging AWS services to process, query, and visualize data. The pipeline integrates Python, AWS S3, AWS Glue, AWS Athena, and AWS QuickSight to build a seamless workflow for extracting insights from raw data.

Workflow
Data Preparation with Python
Raw data is processed using Python and uploaded to Amazon S3 for storage.

Data Transformation with AWS Glue
AWS Glue is used to create ETL (Extract, Transform, Load) scripts that clean and transform the data for analysis.

Cataloging with AWS Glue Data Catalog
Transformed data is registered in the AWS Glue Data Catalog for seamless integration with Athena.

Data Querying with AWS Athena
AWS Athena is used to perform SQL-like queries on the processed data stored in S3.

Data Visualization with AWS QuickSight
Insights generated from Athena queries are visualized in AWS QuickSight to create interactive dashboards and reports.

Key Features
Automated ETL process using AWS Glue.
Serverless SQL querying with AWS Athena.
Scalable and interactive dashboards in AWS QuickSight.
Benefits
Fully serverless and highly scalable architecture.
Cost-efficient querying and visualization tools.
Real-time insights into data with minimal operational overhead.
