import mysql.connector
import json
from mysql.connector import Error

def establish_connection():
    try:
        return mysql.connector.connect(
            host="localhost",
            user="root",
            password="actowiz",
            database="tacoma"
        )
    except Error as e:
        print(f"Database connection error: {e}")
        return None

def initialize_url_table():
    connection = establish_connection()
    if not connection:
        return
    
    cursor = connection.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS product_url_queue (
        id INT AUTO_INCREMENT PRIMARY KEY,
        product_title VARCHAR(500),
        page_url TEXT,
        product_ref_id VARCHAR(255),
        category_id VARCHAR(255),
        process_status VARCHAR(50) DEFAULT 'pending'
    )
    """

    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
    print("Product URL Queue table initialized successfully")

def save_url_records(records):
    if not records:
        return

    connection = establish_connection()
    if not connection:
        print("Failed to connect to database")
        return

    cursor = connection.cursor()

    sql = """
    INSERT INTO product_url_queue (
        product_title,
        page_url,
        product_ref_id,
        category_id,
        process_status
    ) VALUES (%s, %s, %s, %s, %s)
    """

    try:
        cursor.executemany(sql, records)
        connection.commit()
        print(f"Inserted {cursor.rowcount} records into product_url_queue")
    except Error as e:
        print(f"Error inserting records: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()

def initialize_details_table():
    connection = establish_connection()
    if not connection:
        return

    cursor = connection.cursor()

    sql = """
    CREATE TABLE IF NOT EXISTS product_detail_records (
        id INT AUTO_INCREMENT PRIMARY KEY,
        product_id VARCHAR(255),
        category_id VARCHAR(255),
        product_name TEXT,
        product_sku VARCHAR(255),
        list_price VARCHAR(100),
        image_url TEXT,
        weight_info VARCHAR(100),
        full_description LONGTEXT,
        availability_status VARCHAR(255),
        product_attributes LONGTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    cursor.execute(sql)
    connection.commit()
    cursor.close()
    connection.close()
    print("Product detail records table initialized successfully")

def load_pending_urls(queue_table):
    connection = establish_connection()
    if not connection:
        return []

    cursor = connection.cursor()

    sql = f"SELECT page_url FROM {queue_table} WHERE process_status = 'pending'"
    cursor.execute(sql)

    pending_urls = [row[0] for row in cursor.fetchall()]

    cursor.close()
    connection.close()
    print(f"Found {len(pending_urls)} pending URLs")
    return pending_urls

def update_url_status(page_url, status):
    """Update the status of a processed URL"""
    connection = establish_connection()
    if not connection:
        return

    cursor = connection.cursor()
    sql = "UPDATE product_url_queue SET process_status = %s WHERE page_url = %s"
    
    try:
        cursor.execute(sql, (status, page_url))
        connection.commit()
    except Error as e:
        print(f"Error updating URL status: {e}")
    finally:
        cursor.close()
        connection.close()

def save_detail_records(detail_batch):
    if not detail_batch:
        return

    connection = establish_connection()
    if not connection:
        return

    cursor = connection.cursor()

    sql = """
    INSERT INTO product_detail_records (
        product_id,
        category_id,
        product_name,
        product_sku,
        list_price,
        image_url,
        weight_info,
        full_description,
        availability_status,
        product_attributes
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    row_values = []
    for record in detail_batch:
        row_values.append((
            record.get("product_id"),
            record.get("category_id"),
            record.get("product_name"),
            record.get("product_sku"),
            record.get("list_price"),
            record.get("image_url"),
            record.get("weight_info"),
            record.get("full_description"),
            record.get("availability_status"),
            json.dumps(record.get("product_attributes", []), ensure_ascii=False)
        ))

    try:
        cursor.executemany(sql, row_values)
        connection.commit()
        print(f"Saved {len(row_values)} records to product_detail_records")
    except Error as e:
        print(f"Error saving detail records: {e}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()