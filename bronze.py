from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import os
import json

#Loading the dataset
project_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(project_dir, 'data', 'sales_100.csv')
df = pd.read_csv(file_path)

# Add a unique ID column to the DataFrame as the Primary Key
df.columns = df.columns.str.replace(' ', '_')
df['id'] = range(1, len(df) + 1)
df = df[['id'] + [col for col in df.columns if col != 'id']]
df['id'] = df['id'].astype(int)

# Changing the date scheme to agree with Cassandra
df['Order_Date'] = pd.to_datetime(df['Order_Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
df['Ship_Date'] = pd.to_datetime(df['Ship_Date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')


# Establish Connection
sb = os.path.join(project_dir, 'secure-connect-medallion.zip')
cloud_config= {
  'secure_connect_bundle' : sb
}

with open ("Medallion-token.json") as f:
  secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]
auth_provider = PlainTextAuthProvider(CLIENT_ID,CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

if session:
  print('Connected')
else:
  print("An error occured.")


# Using the keyspace
session.set_keyspace("silver")

# Silver Level: Data Cleaning
# Changing the date scheme to agree with Cassandra


# Creating a new table with cleaned date columns
session.execute("""
CREATE TABLE IF NOT EXISTS silver.clean (
 id int PRIMARY KEY,
 "Region" text,
 "Country" text,
 "Item_Type" text,
 "Sales_Channel" text,
 "Order_Priority" text,
 "Order_Date" date,
 "Order_ID" int,
 "Ship_Date" date,
 "UnitsSold" int,
 "UnitPrice" float,
 "UnitCost" float,
 "TotalRevenue" float,
 "TotalCost" float,
 "TotalProfit" float
)
""")


# Updated Insert Query
insert_query = session.prepare("""
INSERT INTO silver.clean (id, Region, Country, Item_Type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, UnitsSold, UnitPrice, UnitCost, TotalRevenue, TotalCost, TotalProfit)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")



# Insert data into the table
for index, row in df.iterrows():
 session.execute(insert_query, (
     row['id'],
     row['Region'],
     row['Country'],
     row['Item_Type'],
     row['Sales_Channel'],
     row['Order_Priority'],
     row['Order_Date'],
     row['Order_ID'],
     row['Ship_Date'],
     row['UnitsSold'],
     row['UnitPrice'],
     row['UnitCost'],
     row['TotalRevenue'],
     row['TotalCost'],
     row['TotalProfit']
 ))




print("Data inserted successfully.")

rows = session.execute("SELECT * FROM silver.clean LIMIT 10")
for row in rows:
   print(row)

# Shutdown the cluster
cluster.shutdown()