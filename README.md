# DataEngineerProjects
# PySpark RDD - Orders Dataset Analysis

This project demonstrates the use of **PySpark RDDs** to perform operations on an orders dataset.

## Dataset
The dataset contains order details with the following columns:
- **order_id** (INT)
- **order_date** (TIMESTAMP as STRING)
- **customer_id** (INT)
- **order_status** (STRING)
## Operations Performed
Using **RDD transformations** and **actions**, the following were implemented:
- Reading data into RDDs
- Splitting records using `map`
- Extracting columns
- Counting orders by status using `map` + `reduceByKey`
- Sorting results using `sortBy`
- Filtering orders by status (e.g., `COMPLETE`, `CLOSED`)
- Aggregations like total order count

## Tech Stack
- Python
- PySpark (RDD API)
