import sqlite3
import pandas as pd
import csv

conn = sqlite3.connect('Data Engineer_ETL Assignment.db')
cursor = conn.cursor()

query = """
SELECT c.customer_id, c.age, i.item_name, SUM(o.quantity) AS total_quantity
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN orders o ON s.sales_id = o.sales_id
JOIN items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
GROUP BY c.customer_id, i.item_id
HAVING total_quantity > 0
"""

cursor.execute(query)
final_output = cursor.fetchall()

csv_file_path = "output1.csv"

with open(csv_file_path, mode='w', newline='') as file:
    writer = csv.writer(file, delimiter = ';')
    writer.writerow(['Customer', 'Age', 'Item', 'Quantity'])
    for row in final_output:
        writer.writerow([row[0], row[1], row[2],row[3]])