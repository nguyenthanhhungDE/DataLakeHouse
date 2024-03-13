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

4. File Format: We store data in the data lake in file formats such as Parquet.
5.Visualize the data using metabase
6.Create a book recommendation app using streamlit


