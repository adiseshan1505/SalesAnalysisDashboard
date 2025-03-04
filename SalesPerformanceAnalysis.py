#%%
import pandas as pd
from sqlalchemy import create_engine, text

#connecting DB details
DB_USERNAME="postgres"
DB_PASSWORD="Adiseshan1505"
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="sales_db"

#creating engine
engine=create_engine(f"postgresql+psycopg2://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

#entering POSQL queries for creation of tables
#tables customers and sales
createTablesQuery="""
create table if not exists customers(customer_id serial primary key, customer_name varchar(100), region varchar(50), age int);

create table if not exists sales(sale_id serial primary key, customer_id int references customers(customer_id), product varchar(100), quantity int, price decimal(10,2), sale_date date);

create table if not exists products (product_id serial primary key, product_name varchar(100), category varchar(50));
"""

#connecting the engine for the creation queries
with engine.connect() as conn:
    conn.execute(text(createTablesQuery))
    conn.commit()

#%%
#entering POSQL queries for inserting data in both tables
insertQueries="""
    insert into customers (customer_name, region, age) values ('alice johnson', 'north america', 30), ('bob smith', 'europe', 40), ('charlie zhang', 'asia', 35), ('robert monero', 'mexico', 35), ('hector munez', 'usa', 50) on conflict (customer_id) do nothing;

    insert into sales (customer_id, product, quantity, price, sale_date) values (1, 'laptop', 2, 50000.00, '2024-02-01'), (2, 'smartphone', 1, 230000.00, '2024-02-05'), (3, 'tablet', 3, 60000.00, '2023-02-10'), (4, 'airpods', 2, 100000.00, '2022-05-10'), (5, 'speakers', 1, 30000.00, '2024-03-12') on conflict (sale_id) do nothing;

    insert into products (product_name, category) values ('laptop', 'electronics'), ('smartphone', 'electronics'), ('tablet', 'electronics'), ('airpods', 'accessories'), ('speakers', 'accessories');
"""
#connecting the engine for the insertion queries
with engine.connect() as conn:
    conn.execute(text(insertQueries))
    conn.commit()
#%%
#Loading the data into Pandas dataframe
dfCustomers=pd.read_sql("select * from customers;", engine)
dfSales=pd.read_sql("select * from sales;", engine)
dfProducts=pd.read_sql("select * from products;", engine)

#returning the head of both the customer and sales dataframe
print("Customers Table:\n",dfCustomers.head())
print("Sales Table:\n",dfSales.head())
print("Products Table:\n",dfProducts.head())
#%%
#Total Revenue Calculation
dfSales["total_revenue"]=dfSales["quantity"]*dfSales["price"]
total_revenue=dfSales["total_revenue"].sum()
print(f"Total revenue from the sales dataset is:- {total_revenue}")
#%%
# Step 6: Merge Data for Power BI Export
df_merged = df_sales.merge(df_customers, on="customer_id", how="left")
df_merged["total_revenue"] = df_merged["quantity"] * df_merged["price"]
df_merged.to_csv("sales_report.csv", index=False)
print("Data exported to sales_report.csv for Power BI visualization!")

#%%
#total sales by region
total_sales_by_region = df_merged.groupby("region")["total_revenue"].sum()
print("Total Sales by Region:\n", total_sales_by_region)

#customer segmentation based on spending
customer_spending = df_merged.groupby('customer_name')['total_revenue'].sum()
print("Customer Spending:\n", customer_spending)
#%%
#Cleaning the data:-
# Check for missing values
missing_data = df_merged.isnull().sum()
print("Missing Data:\n", missing_data)

# Remove duplicate records
df_merged.drop_duplicates(inplace=True)
