# task6_SnowFlake
Build ETL-pipeline based on SnowFlake, using Airflow(local). 

Technical description:
1. Parse file, get rid of the indexes otherwise snowflake won't read it;
2. Create 2 SnowFlake data streams and manage 2 tables there (RAW_TABLE, STAGE_TABLE);
3. Write data from CSV into RAW_TABLE;
4. Write data from RAW_TABLE into STAGE_TABLE;
5. Write data from STAGE_TABLE into MASTER_TABLE.

No additional transformation of data is required. Add logical fragmentation on DAGs (Airflow). Use any Airflow operators and hooks. No Docker, connection to Snowflake either through AIrflow or through the code itself
