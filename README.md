#Introduce
This project is designed to construct a data lakehouse. This data lakehouse will enable organizations to store, manage, and analyze large datasets in a cost-effective, secure, and scalable manner. The data lakehouse will provide a centralized repository for all data, allowing users to easily access and query the data with a unified interface.

Minio will provide distributed object storage to store the data, Delta Lake will provide ACID-compliant transactions for managing the data, Spark will enable distributed computing for analytics, Presto will provide fast SQL queries, and Hive Metastore will provide a unified catalog for the data. This data lakehouse will enable organizations to quickly and easily access and analyze valuable data, allowing them to make better data-driven decisions.

# DataLakeHouse
Tiểu Luận Chuyên Ngành
### System Architecture
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/9cce1acf-b865-4ec5-9897-d846e2909b1b)
### Data Quatity
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/7d84a555-1442-4099-a1b8-3a296c3c614a)
#### Lakehouse Architecture:
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/4b626f3c-539e-4afa-9fcb-6f96605f267a)
#### Prepare infrastructure
docker compose up -d
# 1.WorkFlow
1. We use docker to containerize the application and dagster to orchestrate assets (as defined in dagster's documentation).
2. I am using the Olist e-commerce dataset from Kaggle, importing it into a MySQL database to simulate a real database.
3. In the data lake, we store data following the Medalion architecture as follows:
Bronze Layer: Storing raw data.
Silver Layer: Storing cleansed and conformed data.
Gold Layer: Storing curated business-level tables.
# 2.Loading Strategy
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/5002dd16-50f3-470a-a30d-bf36ca4ccb20)


5. File Format: We store data in the data lake in file formats such as Parquet.
5.Visualize the data using metabase
6.Create a book recommendation app using streamlit


