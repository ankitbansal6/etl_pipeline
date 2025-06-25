from datetime import datetime
import boto3
import email_utility
from mysql_connector import get_cursor

#$ git config --global user.name "John Doe"
#$ git config --global user.email johndoe@example.com

from validator_utils import (
    load_product_master,
    validate_line,
    read_s3_file,
    write_s3_file,
    copy_s3_file,
    delete_s3_file,
    credentials
)


bucket_name = "namastekart"
date_str = datetime.now().strftime("%Y%m%d")
base_prefix = f"incoming_files/{date_str}/"
success_prefix = f"success_files/{date_str}/"
rejected_prefix = f"rejected_files/{date_str}/"
product_master_key = "product_master.csv"


access_key, secret_key, app_key = credentials()


s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)
def list_incoming_files():
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=base_prefix)
    return [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".csv")]

def process_file(file_key, products):
    lines = read_s3_file(bucket_name, file_key)
    header = lines[0].strip()
    data_lines = lines[1:]

    file_valid = True
    rejected_lines = []

    for line in data_lines:
        fields = line.strip().split(",")
        reasons = validate_line(fields, products, datetime.now())
        if reasons:
            file_valid = False
            rejected_lines.append(f"{line.strip()},{';'.join(reasons)}")

    return file_valid, header, rejected_lines, data_lines

def main():
    products = load_product_master(bucket_name, product_master_key)
    files = list_incoming_files()
    subject = (f"Subject: validation email {datetime.now().strftime('%Y-%m-%d')}")
    cursor,conn = get_cursor()
    insert_query = """
        INSERT INTO orders 
        VALUES (%s, %s, %s ,%s, %s, %s)
        """
    total = len(files)
    success = 0
    fail = 0

    if total == 0:
        print(f"No incoming files found for {date_str}.")
        email_utility.send_email_native(
        subject = subject,
        body = "No incoming files",
        sender = "ankitbansal1988@gmail.com",
        password = app_key,  # NOT your regular Gmail password
        recipient = "info@namastesql.com"
        )
        return

    for file_key in files:
        #file_name = os.path.basename(file_key)
        file_name = file_key.split('/')[-1]
        file_valid, header, rejected_lines ,data_lines = process_file(file_key, products)

        if file_valid:
            dest_key = success_prefix + file_name
            copy_s3_file(bucket_name, file_key, dest_key)
            data = [tuple(row.split(',')) for row in data_lines]
            cursor.executemany(insert_query, data)
            success += 1
        else:
            dest_key = rejected_prefix + file_name
            copy_s3_file(bucket_name, file_key, dest_key)
            error_key = rejected_prefix + f"error_{file_name}"
            write_s3_file(bucket_name, error_key, [header + ",rejection_reason"] + rejected_lines)
            fail += 1

        delete_s3_file(bucket_name, file_key)

    
    conn.commit()
    cursor.close()
    conn.close()

    body = f"""Total {total} incoming files processed.
    {success} files passed validation.
    {fail} files failed validation."""

    # Example usage
    email_utility.send_email_native(
        subject = subject,
        body = body,
        sender = "ankitbansal1988@gmail.com",
        password = app_key,  # NOT your regular Gmail password
        recipient = "info@namastesql.com"
    )

if __name__ == "__main__":
    main()
