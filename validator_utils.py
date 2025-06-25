
import boto3
from datetime import datetime
import configparser


def credentials():
    
    # Step 1: Create a ConfigParser object
    config = configparser.ConfigParser()

    # Step 2: Read the config file
    config.read('config.ini')
    access_key = config['aws']["access_key"]
    secret_key = config["aws"]["secret_key"]
    app_key = config['gmail_app_password']["pass_key"]
    return access_key , secret_key , app_key

access_key, secret_key, app_key = credentials()

s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

def load_product_master(bucket_name, master_key):
    products = {}
    response = s3.get_object(Bucket=bucket_name, Key=master_key)
    lines = response['Body'].read().decode('utf-8').splitlines()

    for line in lines[1:]:
        parts = line.strip().split(",")
        if len(parts) == 4:
            product_id, product_name, price, category = parts
            products[product_id] = float(price)
    return products

def validate_line(fields, products, current_date):
    reasons = []
    if len(fields) != 6:
        reasons.append("Incorrect number of fields")
        return reasons

    order_id, order_date, product_id, quantity, total_sales_amount, city = [f.strip() for f in fields]

    for field in fields:
        if field == "":
            reasons.append("Empty fields present")

    if product_id not in products:
        reasons.append("Invalid product ID")

    try:
        quantity_val = int(quantity)
        amount_val = float(total_sales_amount)
        if product_id in products:
            expected_amount = products[product_id] * quantity_val
            if round(expected_amount, 2) != round(amount_val, 2):
                reasons.append("Incorrect total_sales_amount")
    except ValueError:
        reasons.append("Invalid quantity or total_sales_amount format")

    try:
        order_date_val = datetime.strptime(order_date, "%d-%m-%Y")
        if order_date_val > current_date:
            reasons.append("Order date is in the future")
    except:
        reasons.append("Invalid order_date format")

    if city not in ["Mumbai", "Bangalore"]:
        reasons.append("Invalid city")

    return reasons

def read_s3_file(bucket_name, key):
    response = s3.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read().decode('utf-8')
    return content.splitlines()

def write_s3_file(bucket_name, key, lines):
    s3.put_object(Bucket=bucket_name, Key=key, Body="\n".join(lines).encode('utf-8'))

def copy_s3_file(bucket_name, src_key, dest_key):
    s3.copy_object(Bucket=bucket_name, CopySource={'Bucket': bucket_name, 'Key': src_key}, Key=dest_key)

def delete_s3_file(bucket_name, key):
    s3.delete_object(Bucket=bucket_name, Key=key)
