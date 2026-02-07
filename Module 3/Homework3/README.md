## Module 3 Homework

    -- External Table
    CREATE OR REPLACE EXTERNAL TABLE `project_id.2024_yellow_taxi_data.external_yellow_tripdata`
    OPTIONS (
        format = 'PARQUET',
        uris = ['gs://dez_hw3_2026_sm/*.parquet']
    );

    -- Regular Table
    CREATE OR REPLACE TABLE `project_id.2024_yellow_taxi_data.regular_yellow_tripdata`
    AS
    SELECT * FROM `project_id.2024_yellow_taxi_data.external_yellow_tripdata`;

1. What is count of records for the 2024 Yellow Taxi Data?
   - 20,332,093

   ```
   SELECT COUNT(1) FROM `project_id.2024_yellow_taxi_data.external_yellow_tripdata`;
   ```

2. What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
   - 0 MB for the External Table and 155.12 MB for the Materialized Table

   ```
   SELECT COUNT(DISTINCT(PULocationID)) FROM `project_id.2024_yellow_taxi_data.external_yellow_tripdata`;
   ```

3. Why are the estimated number of Bytes different?
   - BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.

4. How many records have a fare_amount of 0?
   - 8,333

   ```
   SELECT COUNT(1) FROM `project_id.2024_yellow_taxi_data.regular_yellow_tripdata` WHERE fare_amount = 0;
   ```

5. What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID ?
   - Partition by tpep_dropoff_datetime and Cluster on VendorID

   ```
   CREATE OR REPLACE TABLE `project_id.2024_yellow_taxi_data.partitioned_clustered_yellow_tripdata`
   PARTITION BY DATE(tpep_dropoff_datetime)
   CLUSTER BY VendorId AS
   SELECT * FROM `project_id.2024_yellow_taxi_data.external_yellow_tripdata`;
   ```

6. Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive). Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. What are these values?
   - 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table

   ```
   SELECT DISTINCT(VendorID) FROM `project_id.2024_yellow_taxi_data.regular_yellow_tripdata`
   WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

   SELECT DISTINCT(VendorID) FROM `project_id.2024_yellow_taxi_data.partitioned_clustered_yellow_tripdata`
   WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';
   ```

7. Where is the data stored in the External Table you created? (1 point)
   - GCP Bucket

8. It is best practice in Big Query to always cluster your data: (1 point)
   - True
