import psycopg2
import sys
import os

sys.path.append(os.path.abspath("/opt/airflow/dags/scripts"))
from config_etl import oltp_conn_string

# PostgreSQL credentials (modify based on your setup)
DB_HOST = oltp_conn_string['host']
DB_PORT = oltp_conn_string['port']
DB_USER = oltp_conn_string['user']
DB_PASSWORD = oltp_conn_string['password']
DB_NAME = oltp_conn_string['database']  # Database name for OLTP

def execute_query(connection, query):
    """Executes a SQL query."""
    with connection.cursor() as cursor:
        cursor.execute(query)
    connection.commit()

def run_ddl_oltp():
    # Step 1: Connect to PostgreSQL (default database)
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname="postgres"  # Connect to default DB to drop/create `project3_dwh`
        )
        conn.autocommit = True
        print("Connected to PostgreSQL successfully!")

        # Step 2: Drop and Recreate Database
        execute_query(conn, f"DROP DATABASE IF EXISTS {DB_NAME}")
        execute_query(conn, f"CREATE DATABASE {DB_NAME}")
        print(f"Database `{DB_NAME}` has been recreated.")

        conn.close()

        # Step 3: Connect to the new database
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME
        )
        print(f"Connected to `{DB_NAME}` successfully!")

        # Step 4: Create Tables
        create_tables_query = """

CREATE TABLE tb_users (
	user_id INT NOT NULL,
	user_first_name VARCHAR(255) NOT NULL,
	user_last_name VARCHAR(255) NOT NULL,
	user_gender VARCHAR(50) NOT NULL,
	user_address VARCHAR(255),
	user_birthday DATE NOT NULL,
	user_join DATE NOT NULL,
	PRIMARY KEY (user_id)
);
	
CREATE TABLE tb_payments (
	payment_id INT NOT NULL,
	payment_name VARCHAR(255) NOT NULL,
	payment_status BOOLEAN NOT NULL,
	PRIMARY KEY (payment_id)
);
	
CREATE TABLE tb_shippers (
	shipper_id INT NOT NULL,
	shipper_name VARCHAR(255) NOT NULL,
	PRIMARY KEY (shipper_id)
);
	
CREATE TABLE tb_ratings (
	rating_id INT NOT NULL,
	rating_level INT NOT NULL,
	rating_status VARCHAR(255) NOT NULL,
	PRIMARY KEY (rating_id)
);
	
CREATE TABLE tb_product_category (
	product_category_id INT NOT NULL,
	product_category_name VARCHAR(255) NOT NULL,
	PRIMARY KEY (product_category_id)
);
	
CREATE TABLE tb_vouchers (
	voucher_id INT NOT NULL,
	voucher_name VARCHAR(255) NOT NULL,
	voucher_price INT,
	voucher_created DATE NOT NULL,
	user_id INT NOT NULL,
	PRIMARY KEY (voucher_id),
	CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES tb_users (user_id)
);
	
CREATE TABLE tb_orders (
	order_id INT NOT NULL,
	order_date DATE NOT NULL,
	user_id INT NOT NULL,
	payment_id INT NOT NULL,
	shipper_id INT NOT NULL,
	order_price INT NOT NULL,
	order_discount INT,
	voucher_id INT,
	order_total INT NOT NULL,
	rating_id INT NOT NULL,
	PRIMARY KEY (order_id),
	CONSTRAINT fk_user_id FOREIGN KEY (user_id) REFERENCES tb_users (user_id),
	CONSTRAINT fk_payment_id FOREIGN KEY (payment_id) REFERENCES tb_payments (payment_id),
	CONSTRAINT fk_shipper_id FOREIGN KEY (shipper_id) REFERENCES tb_shippers (shipper_id),
	CONSTRAINT fk_rating_id FOREIGN KEY (rating_id) REFERENCES tb_ratings (rating_id)
);
	
CREATE TABLE tb_products (
	product_id INT NOT NULL,
	product_category_id INT NOT NULL,
	product_name VARCHAR(255) NOT NULL,
	product_created DATE NOT NULL,
	product_price INT NOT NULL,
	product_discount INT,
	PRIMARY KEY (product_id),
	CONSTRAINT fk_product_category_id FOREIGN KEY (product_category_id) REFERENCES tb_product_category (product_category_id)
);
	
CREATE TABLE tb_order_items (
	order_item_id INT NOT NULL ,
	order_id INT NOT NULL,
	product_id INT NOT NULL,
	order_item_quantity INT,
	product_discount INT,
	product_subdiscount INT,
	product_price INT NOT NULL,
	product_subprice INT NOT NULL,
	PRIMARY KEY (order_item_id),
	CONSTRAINT fk_product_id FOREIGN KEY (product_id) REFERENCES tb_products (product_id),
	CONSTRAINT fk_order_id FOREIGN KEY (order_id) REFERENCES tb_orders (order_id)
);

INSERT INTO tb_users (user_id,user_first_name,user_last_name,user_gender,user_address,user_birthday,user_join) VALUES 
(100101,'Budi','Gunawan','Male','Jl. Pondok Indah No.1, Kecamatan Pondok Labu, Bandung Raya, Jawa Barat','1998-09-12','2022-01-13'),
(100102,'Eva','Susanti','Female','Jl. Timur Raya No. 13, Kramat Jaya, Jakarta Timur, DKI Jakarta','1997-02-16','2022-01-29'),
(100103,'Dana','Pradana','Male','Jl. Pahlawan, Surabaya, Jawa Timur','1999-07-19','2022-02-11'),
(100104,'Rahmat','Hidayat','Male','Jl. Amil Abas, Jakarta Timur, DKI Jakarta','2000-02-14','2022-03-22'),
(100105,'Dodo','Andriano','Male','Jl. Pakuan Selatan No. 177, Magelang, Jawa Tengah','2000-09-06','2022-04-03'),
(100106,'Caca','Kumala','Female','Jl. Bunga Raya, Kota Tanggerang, Banten','1998-11-05','2022-05-20'),
(100107,'Andi','Kurniawan','Male','Jl. Mawar Indah No. 25, Jakarta Barat, DKI Jakarta','2001-03-14','2022-05-24'),
(100108,'Fanny','Utami','Female','Jl. Kilometer Panjang No. 210, Jakarta Utara, DKI Jakarta','2002-01-27','2022-06-02'),
(100109,'Gagah','Prakasa','Male','Jl. Timur Asri No. 10, Denpasar, Bali','2001-08-05','2022-07-14'),
(100110,'Anita','Friska','Female','Jl. Tembung Raya, Kota Medan Timur, Sumatera Utara','2000-11-04','2022-07-21');

INSERT INTO tb_payments (payment_id,payment_name,payment_status) VALUES
(1201,'Cash',FALSE),
(1202,'Debit',TRUE),
(1203,'Wallet',TRUE),
(1204,'Credit',TRUE);

INSERT INTO tb_shippers (shipper_id,shipper_name) VALUES
(60002001,'JNE Express'),
(60002002,'Sicepat Express'),
(60002003,'Lazada Express');

INSERT INTO tb_ratings (rating_id,rating_level,rating_status) VALUES
(800010001,1,'Very Low Impact'),
(800010002,2,'Low Impact'),
(800010003,3,'Medium Impact'),
(800010004,4,'Medium High Impact'),
(800010005,5,'High Impact');

INSERT INTO tb_product_category (product_category_id,product_category_name) VALUES
(320001001,'Fashion'),
(320001002,'Electronic'),
(320001003,'Health & Beauty');

INSERT INTO tb_vouchers (voucher_id,voucher_name,voucher_price,voucher_created,user_id) VALUES
(41000101,'New User',5000,'2022-01-13',100101),
(41000102,'New User',5000,'2022-01-29',100102),
(41000103,'New User',5000,'2022-02-11',100103),
(41000104,'New User',5000,'2022-03-22',100104),
(41000105,'New User',5000,'2022-04-03',100105),
(41000106,'New User',5000,'2022-05-20',100106),
(41000107,'New User',5000,'2022-05-24',100107),
(41000108,'New User',5000,'2022-06-02',100108),
(41000109,'New User',5000,'2022-07-14',100109),
(41000110,'New User',5000,'2022-07-21',100110),
(41000111,'Bag Promo',20000,'2022-02-01',100101),
(41000112,'Bag Promo',20000,'2022-02-01',100102),
(41000113,'Bag Promo',20000,'2022-03-01',100103),
(41000114,'Body Soap Promo',10000,'2022-03-01',100108),
(41000115,'Body Soap Promo',10000,'2022-07-20',100107);

INSERT INTO tb_orders (order_id,order_date,user_id,payment_id,shipper_id,order_price,order_discount,voucher_id,order_total,rating_id) VALUES
(1110001,'2022-01-20',100101,1202,60002001,250000,15000,41000101,230000,800010003),
(1110002,'2022-01-29',100102,1202,60002001,620000,40000,41000102,575000,800010003),
(1110003,'2022-02-13',100103,1204,60002001,6000000,1000000,41000103,4995000,800010001),
(1110004,'2022-03-06',100102,1203,60002001,3150000,45000,NULL,3105000,800010005),
(1110005,'2022-04-28',100105,1202,60002002,4000000,1000000,41000105,2995000,800010001),
(1110006,'2022-05-09',100103,1202,60002002,4500000,1030000,NULL,3470000,800010004),
(1110007,'2022-05-21',100106,1202,60002001,870000,25000,NULL,845000,800010005),
(1110008,'2022-06-02',100108,1204,60002002,2000000,0,41000108,1995000,800010004),
(1110009,'2022-06-23',100103,1204,60002003,2000000,0,NULL,2000000,800010005),
(1110010,'2022-07-01',100102,1204,60002003,1050000,45000,NULL,1005000,800010002),
(1110011,'2022-07-21',100110,1203,60002002,550000,15000,NULL,535000,800010005),
(1110012,'2022-07-30',100110,1202,60002001,490000,35000,41000115,445000,800010004);

INSERT INTO tb_products (product_id,product_category_id,product_name,product_created,product_price,product_discount) VALUES
(31110001,320001001,'Bag','2022-01-01',300000,0),
(31110002,320001001,'Shirt','2022-01-01',250000,15000),
(31110003,320001002,'Camera','2022-01-10',1800000,0),
(31110004,320001002,'Television','2022-01-11',2000000,0),
(31110005,320001002,'Headphone','2022-01-12',4000000,1000000),
(31110006,320001003,'Supplement','2022-01-12',500000,0),
(31110007,320001003,'Body Soap','2022-01-13',120000,10000);

INSERT INTO tb_order_items (order_item_id,order_id,product_id,order_item_quantity,product_discount,product_subdiscount,product_price,product_subprice) VALUES
(90010001,1110001,31110002,1,15000,15000,250000,250000),
(90010002,1110002,31110007,1,10000,10000,120000,120000),
(90010003,1110002,31110002,2,15000,30000,250000,500000),
(90010004,1110003,31110004,1,0,0,2000000,2000000),
(90010005,1110003,31110005,1,1000000,1000000,4000000,4000000),
(90010006,1110004,31110001,2,0,0,300000,600000),
(90010007,1110004,31110002,3,15000,45000,250000,750000),
(90010008,1110004,31110003,1,0,0,1800000,1800000),
(90010009,1110005,31110005,1,1000000,1000000,4000000,4000000),
(90010010,1110006,31110005,1,1000000,1000000,4000000,4000000),
(90010011,1110006,31110002,2,15000,30000,250000,500000),
(90010012,1110007,31110006,1,0,0,500000,500000),
(90010013,1110007,31110007,1,10000,10000,120000,120000),
(90010014,1110007,31110002,1,15000,15000,250000,250000),
(90010015,1110008,31110004,1,0,0,2000000,2000000),
(90010016,1110009,31110004,1,0,0,2000000,2000000),
(90010017,1110010,31110002,3,15000,45000,250000,750000),
(90010018,1110010,31110001,1,0,0,300000,300000),
(90010019,1110011,31110001,1,0,0,300000,300000),
(90010020,1110011,31110002,1,15000,15000,250000,250000),
(90010021,1110012,31110002,1,15000,15000,250000,250000),
(90010022,1110012,31110007,2,10000,20000,120000,240000);
        """
        execute_query(conn, create_tables_query)
        print("Tables have been created successfully!")

    except Exception as e:
        print("Error:", e)

    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Connection closed.")
