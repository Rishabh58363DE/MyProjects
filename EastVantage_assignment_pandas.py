import sqlite3
import pandas as pd
import csv

conn = sqlite3.connect('Data Engineer_ETL Assignment.db')

sales_df = pd.read_sql_query("SELECT * FROM sales", conn)
customer_df = pd.read_sql_query("SELECT * FROM customers", conn)
orders_df = pd.read_sql_query("SELECT * FROM orders", conn)
items_df = pd.read_sql_query("SELECT * FROM items",conn)

merged_df = pd.merge(customer_df, sales_df, on='customer_id')
merged_df = pd.merge(merged_df, orders_df, on='sales_id')
merged_df = pd.merge(merged_df, items_df, on='item_id')

filtered_df = merged_df[(merged_df['age'] >= 18) & (merged_df['age'] <= 35)]
sum_df = filtered_df.groupby(['customer_id', 'age', 'item_name'])['quantity'].sum().reset_index()

sum_df = sum_df[sum_df['quantity'] > 0]

csv_file_path = "output_pandas.csv"

sum_df.to_csv(csv_file_path, sep=';', index=False, header = ['Customer', 'Age', 'Item', 'Quantity'])
