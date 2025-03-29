import os

# Koneksi ke Database OLTP (Sumber Data)
default_conn_string = {
    'user': os.getenv('OLTP_DB_USER'),
    'password': os.getenv('OLTP_DB_PASSWORD'),
    'host': os.getenv('OLTP_DB_HOST'),
    'port': os.getenv('OLTP_DB_PORT'),
    'database': 'postgres'
}

oltp_conn_string = {
    'user': os.getenv('OLTP_DB_USER'),
    'password': os.getenv('OLTP_DB_PASSWORD'),
    'host': os.getenv('OLTP_DB_HOST'),
    'port': os.getenv('OLTP_DB_PORT'),
    'database': os.getenv('OLTP_DB_NAME')
}

# Koneksi ke Data Warehouse (Tujuan ETL)
warehouse_conn_string = {
    'user': os.getenv('DWH_DB_USER'),
    'password': os.getenv('DWH_DB_PASSWORD'),
    'host': os.getenv('DWH_DB_HOST'),
    'port': os.getenv('DWH_DB_PORT'),
    'database': os.getenv('DWH_DB_NAME')
}

# Mapping antara tabel sumber (OLTP) dan tabel tujuan (Data Warehouse)
table_mapping = {
    "users": {"source": "tb_users", "destination": "dim_user"},
    "payments": {"source": "tb_payments", "destination": "dim_payment"},
    "shippers": {"source": "tb_shippers", "destination": "dim_shipper"},
    "ratings": {"source": "tb_ratings", "destination": "dim_rating"},
    "vouchers": {"source": "tb_vouchers", "destination": "dim_voucher"},
    "orders": {
        "source": ["tb_orders", "tb_users", "tb_payments", "tb_shippers", "tb_ratings", "tb_vouchers"],
        "destination": "fact_orders"
    }
}

oltp_tables = {
    "users": "tb_users",
    "payments": "tb_payments",
    "shippers": "tb_shippers",
    "ratings": "tb_ratings",
    "vouchers": "tb_vouchers",
    "orders": "tb_orders"
}

warehouse_tables = {
    "users": "dim_user",
    "payments": "dim_payment",
    "shippers": "dim_shipper",
    "ratings": "dim_rating",
    "vouchers": "dim_voucher",
    "orders": "fact_orders"
}

# Mapping dari sumber (OLTP) ke tujuan (Data Warehouse)
etl_config = {
    "users": {
        "source_table": "tb_users",
        "destination_table": "dim_user",
        "column_mapping": {
            "user_id": "user_id",
            "user_first_name": "user_first_name",
            "user_last_name": "user_last_name",
            "user_gender": "user_gender",
            "user_address": "user_address",
            "user_birthday": "user_birthday",
            "user_join": "user_join"
        },
        "query": "SELECT * FROM tb_users"
    },
    "payments": {
        "source_table": "tb_payments",
        "destination_table": "dim_payment",
        "column_mapping": {
            "payment_id": "payment_id",
            "payment_name": "payment_name",
            "payment_status": "payment_status"
        },
        "query": "SELECT * FROM tb_payments"
    },
    "shippers": {
        "source_table": "tb_shippers",
        "destination_table": "dim_shipper",
        "column_mapping": {
            "shipper_id": "shipper_id",
            "shipper_name": "shipper_name"
        },
        "query": "SELECT * FROM tb_shippers"
    },
    "ratings": {
        "source_table": "tb_ratings",
        "destination_table": "dim_rating",
        "column_mapping": {
            "rating_id": "rating_id",
            "rating_level": "rating_level",
            "rating_status": "rating_status"
        },
        "query": "SELECT * FROM tb_ratings"
    },
    "vouchers": {
        "source_table": "tb_vouchers",
        "destination_table": "dim_voucher",
        "column_mapping": {
            "voucher_id": "voucher_id",
            "voucher_name": "voucher_name",
            "voucher_price": "voucher_price",
            "voucher_created_date": "voucher_created_date",
            "user_id": "user_id"
        },
        "query": "SELECT * FROM tb_vouchers"
    },
    "orders": {
        "source_table": ["tb_orders", "tb_users", "tb_payments", "tb_shippers", "tb_ratings", "tb_vouchers"],
        "destination_table": "fact_orders",
        "column_mapping": {
            "order_id": "order_id",
            "order_date": "order_date",
            "user_id": "user_id",
            "payment_id": "payment_id",
            "shipper_id": "shipper_id",
            "order_price": "order_price",
            "order_discount": "order_discount",
            "voucher_id": "voucher_id",
            "total": "order_total",
            "rating_id": "rating_id"
        },
        "query": """
            SELECT 
                o.order_id,
                o.order_date,
                o.user_id,
                p.payment_id,
                s.shipper_id,
                o.order_price AS order_price,
                o.order_discount AS order_discount,
                o.order_total AS order_total,
                o.voucher_id,
                r.rating_id
            FROM tb_orders o
            JOIN tb_users u ON o.user_id = u.user_id
            JOIN tb_payments p ON o.payment_id = p.payment_id
            JOIN tb_shippers s ON o.shipper_id = s.shipper_id
            JOIN tb_ratings r ON o.rating_id = r.rating_id
            LEFT JOIN tb_vouchers v ON o.voucher_id = v.voucher_id
        """
    }
}
