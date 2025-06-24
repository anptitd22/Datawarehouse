# Datawarehouse Web gia dụng nguyenkim
Sử dụng ngôn ngữ Python

Sử dụng database Postgre -> SQLserver

Data Web:

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750700441/Screenshot_2025-06-24_003957_xi6xcl.png)

Data Warehouse:

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750651954/Screenshot_2025-06-23_111057_nt8s8m.png)

Star Schema (mô hình hình sao) là một mô hình dữ liệu đơn giản và hiệu quả cho kho dữ liệu (data warehouse), bao gồm bảng thực tế trung tâm (fact table) và nhiều bảng chiều (dimension tables) xung quanh

Chạy trên Docker

Quy trình ETL 

source_code: https://github.com/anptitd22/Datawarehouse/tree/main/dags

Kết hợp công nghệ Spark + Kafka + Protobuf + Schema Registry + Airflow

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750618417/Screenshot_2025-06-23_015216_ziazhg.png)

Sử dụng Spark để extract và transform dữ liệu, mã hóa dữ liệu nhị phân bằng protobuf và truyền trên kênh kafka,
các dữ liệu được đảm bảo trật tự, toàn vẹn bởi schema registry, thực hiện định kì bởi airflow

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750618416/ok_apdjoj.png)

Luồng ETL hóa trên Airflow:

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750652904/ok123_lld8la.png)

Kết quả (doanh thu theo tháng):

![alt](https://res.cloudinary.com/dyzdqrbw8/image/upload/v1750652904/ok664646_wkn87i.png)

Phân tích dự đoán tăng trưởng Machine Learning

# Hướng dẫn cài đặt

Cài đặt thư viện python (3.12)

pip install -r requirements.txt

Cài đặt docker

docker compose build 

#Lưu ý: các thư viện sqlserver trong dockerfile chỉ cài 1 lần lần sau build lại không cần cài nữa trừ khi xóa hết dữ liệu

docker compose up -d

docker compose up

Đăng nhập airflow 

tk: airflow mk: airflow

cài đặt connections trong airflow: postgre và sqlserver

#Lưu ý là nếu dùng docker chứa database thì nên cho vào cùng network airflow

#Trong project này thì dùng postgres bằng docker trong backend web và datamart tự build

#Dữ liệu được cào từ web https://www.nguyenkim.com

#Source_code cào dữ liệu: https://drive.google.com/file/d/18uaifKuMr0RU8YYY3l1UpW11jmTShMLi/view?usp=drive_link

#data postgre: https://github.com/anptitd22/Datawarehouse/blob/main/note/downdata.txt

#script datawarehouse: https://github.com/anptitd22/Datawarehouse/blob/main/note/create_datawarehouse.sql

#Tran Duc AN
