# Datawarehouse Điện máy xanh
Sử dụng ngôn ngữ Python

Sử dụng database Postgre -> SQLserver

Data Warehouse:

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750651954/Screenshot_2025-06-23_111057_nt8s8m.png)

Star Schema (mô hình hình sao) là một mô hình dữ liệu đơn giản và hiệu quả cho kho dữ liệu (data warehouse), bao gồm một bảng thực tế trung tâm (fact table) và nhiều bảng chiều (dimension tables) xung quanh

Chạy trên Docker

Quy trình ETL 

Kết hợp công nghệ Spark + Kafka + Protobuf + Schema Registry + Airflow

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750618417/Screenshot_2025-06-23_015216_ziazhg.png)

Sử dụng Spark để extract và transform dữ liệu, mã hóa dữ liệu nhị phân bằng protobuf và truyền trên kênh kafka,
các dữ liệu được đảm bảo trật tự, toàn vẹn bởi schema registry, thực hiện định kì bởi airflow

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750618416/ok_apdjoj.png)

Phân tích dự đoán tăng trưởng Machine Learning

# Hướng dẫn cài đặt

Cài đặt thư viện python (3.12)

pip install -r requirements.txt

Cài đặt docker

docker compose build 

docker compose up -d

docker compose up

Đăng nhập airflow 

tk: airflow mk: airflow

cài đặt connections database đầu vào và đầu ra

#Lưu ý là nếu dùng docker chứa database thì nên cho vào cùng network airflow

#Trong project này thì dùng postgres bằng docker trong backend web và datamart có script build sẵn

#data postgre: https://github.com/anptitd22/Datawarehouse/blob/main/note/downdata.txt

#script datawarehouse: https://github.com/anptitd22/Datawarehouse/blob/main/note/create_datawarehouse.sql

#Tran Duc AN
