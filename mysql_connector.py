#pip install mysql-connector-python 
import mysql.connector

def get_cursor():
    # Establish the connection
    conn = mysql.connector.connect(
        host="127.0.0.1",        # or your DB host
        user="root",
        password="mysql123",
        database="sys"
    )

    # Create a cursor object
    cursor = conn.cursor()
    return cursor,conn

# # Example query
# cursor.execute("SELECT * FROM employees")
# #   print(cursor.fetchall())
# # Fetch and print results
# # for row in cursor.fetchall():
# #     print(row)

# # Close the connection
# cursor.close()
# #conn.close()

# ## writing back
# cursor = conn.cursor()

# # Define your INSERT query
# insert_query = """
# INSERT INTO employees 
# VALUES (%s, %s, %s)
# """

# # Data to insert
# data = (107, "Ankit", "ankit@gmail.com")

# # Execute and commit
# #cursor.execute(insert_query, data)

# data = [
#     (1, "Alice", "HR"),
#     (2, "Bob", "Finance"),
#     (3, "Charlie", "Tech"),
#     (4, "David", "Marketing")
# ]
# cursor.executemany(insert_query, data)

# conn.commit()

# print("Data inserted successfully")

# # Close connections
# cursor.close()
# conn.close()
