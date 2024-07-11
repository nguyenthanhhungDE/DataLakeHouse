#Introduce
This project is designed to construct a data lakehouse. This data lakehouse will enable organizations to store, manage, and analyze large datasets in a cost-effective, secure, and scalable manner. The data lakehouse will provide a centralized repository for all data, allowing users to easily access and query the data with a unified interface.

Minio will provide distributed object storage to store the data, Delta Lake will provide ACID-compliant transactions for managing the data, Spark will enable distributed computing for analytics, Presto will provide fast SQL queries, and Hive Metastore will provide a unified catalog for the data. This data lakehouse will enable organizations to quickly and easily access and analyze valuable data, allowing them to make better data-driven decisions.

# DataLakeHouse
Tiểu Luận Chuyên Ngành
### System Architecture
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/bc9395b5-dfd8-40a9-9f6d-ad6d228e3aa8)

### Data Quality
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/7d84a555-1442-4099-a1b8-3a296c3c614a)
###### 🥉 𝗕𝗿𝗼𝗻𝘇𝗲 𝗟𝗮𝘆𝗲𝗿: Đây là nơi đầu tiên tiếp nhận dữ liệu thô. Có thể coi đây như là môi trường tự nhiên của dữ liệu, chưa được chạm vào và xử lý.
###### 🥈 𝗦𝗶𝗹𝘃𝗲𝗿 𝗟𝗮𝘆𝗲𝗿: Sau khi trải qua quá trình clean, validation và schema systematization, dữ liệu sẽ được chuyển đến Lớp Bạc. Tại đây, dữ liệu được mài giũa và chuẩn bị sẵn sàng để Analysis, trở thành nguồn tài nguyên đáng tin cậy cho những hiểu biết sâu sắc hơn.
###### 🥇 𝗚𝗼𝗹𝗱 𝗟𝗮𝘆𝗲𝗿: Đây là giai đoạn tinh chế cuối cùng, nơi dữ liệu được aggregated, modeled, and tailored (điều chỉnh) để phù hợp với các kịch bản kinh doanh khác nhau. Trong Lớp Vàng, dữ liệu đạt được giá trị cao nhất, trở thành nguồn tài nguyên trực tiếp và thiết yếu cho strategic decision-making.
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
Trong lĩnh vực cơ sở dữ liệu, các thuật ngữ "append", "overwrite", và "upsert" đều liên quan đến cách thức xử lý dữ liệu khi thêm mới hoặc cập nhật dữ liệu trong một tập hợp đã tồn tại. Dưới đây là sự khác biệt chính giữa chúng:

+ Append (Thêm mới):
Khi sử dụng chế độ append, dữ liệu mới được thêm vào cuối của tập hợp hiện có.
Không có sự thay đổi hoặc ghi đè vào dữ liệu đã tồn tại.
+ Overwrite (Ghi đè):
Trong chế độ ghi đè, dữ liệu mới sẽ ghi đè lên dữ liệu đã tồn tại trong tập hợp.
Dữ liệu hiện có sẽ bị thay thế hoàn toàn bằng dữ liệu mới.
+ Upsert (Kết hợp Cập nhật và Thêm mới):
Khi thực hiện upsert, hệ thống sẽ kiểm tra xem dữ liệu mới có tồn tại trong tập hợp hay không.
Nếu dữ liệu mới đã tồn tại, nó sẽ được cập nhật.
Nếu dữ liệu mới không tồn tại, nó sẽ được thêm mới vào tập hợp.
Về mặt chức năng, append giữ nguyên dữ liệu đã có và chỉ thêm mới dữ liệu mới, overwrite ghi đè hoàn toàn lên dữ liệu đã có, trong khi upsert kết hợp cả việc cập nhật và thêm mới.

Trong kiến trúc Data Lakehouse, có thể sử dụng các chiến lược khác nhau cho các lớp dữ liệu khác nhau như bạn đã nêu. Cụ thể:
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/ede03e55-1231-4d04-aa99-db770883a842)


+ Lớp Bronze:
Thường là lớp chứa dữ liệu gốc, raw data, không làm thay đổi dữ liệu đã có.
Thường sử dụng chiến lược append để thêm dữ liệu mới vào mà không làm thay đổi dữ liệu hiện có.
+ Lớp Silver:
Lớp này thường đã trải qua quá trình tiền xử lý và làm sạch dữ liệu.
Có thể sử dụng chiến lược overwrite để cập nhật dữ liệu trong lớp Silver bằng dữ liệu đã được xử lý mới nhất từ lớp Bronze.
+ Lớp Gold:
Đây thường là lớp dữ liệu đã được xử lý hoàn chỉnh và được sử dụng cho mục đích phân tích và báo cáo.
Có thể sử dụng chiến lược upsert để cập nhật dữ liệu trong lớp Gold, kết hợp cả việc cập nhật dữ liệu đã tồn tại và thêm mới dữ liệu khi cần thiết.
# Data Lineage:
Bronze -> Silver -> Gold -> Platitum
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/3a53a80b-4ea1-4c91-8c81-a0ea879e987f)
+ Bronze:
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/08550af1-47e7-49ef-8489-d4042e8e4467)
+ Silver:
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/722a58ee-97cd-4a82-b54c-04f324f1f3f8)
+ Gold:
![image](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/4e530209-b520-4108-a82e-cf88a5e54dc2)






5. File Format: We store data in the data lake in file formats such as Parquet.
5.Visualize the data using metabase
![z5079403472495_9bdee68cd5aed330cc4dc38517df3666](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/36e3ea76-765d-4e74-b4b4-bb1e3f262d5b)
![z5079403467324_439a49510a0dde79139ff679b4ae57e2](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/36c76d89-d28f-463f-b222-2ff74936c890)
![z5079403470972_4e6625fb275595a235924b5f94a35b94](https://github.com/nguyenthanhhungDE/DataLakeHouse/assets/134383281/5a5820e0-6b67-45b1-8984-ad39d8a03ac5)


6.Create a book recommendation app using streamlit


