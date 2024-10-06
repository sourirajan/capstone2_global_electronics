import pandas as pd
import numpy as np
import datetime
import os
import psycopg2
from sqlalchemy import create_engine, Column, Integer, Float, Date, String, Table, MetaData, ForeignKey, ForeignKeyConstraint
from decimal import Decimal

def get_connection():
    user=os.environ['PG_USER']
    password=os.environ['PG_PASSWORD']
    host=os.environ['PG_HOST']
    port=os.environ['PG_PORT']
    database=os.environ['PG_DBNAME']
    
    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    
    metadata = MetaData()
    
    stores_table = Table('cp2_stores', metadata,
        Column('store_key', Integer, primary_key=True),
        Column('store_country', String, nullable=False),
        Column('store_state', String, nullable=False),
        Column('store_size', Float, nullable=True),
        Column('store_open_date', Date, nullable=False)
    )    

    stores_table = Table('cp2_products', metadata,
        Column('product_key', Integer, primary_key=True),
        Column('product_name', String, nullable=False),
        Column('product_brand', String, nullable=False),
        Column('product_color', String, nullable=False),
        Column('sub_category_key', Integer, nullable=False),
        Column('sub_category', String, nullable=False),
        Column('category_key', Integer, nullable=False),
        Column('product_category', String, nullable=False),
        Column('unit_price', Float, nullable=False),
        Column('unit_cost', Float, nullable=False)
    )

    customers_table = Table('cp2_customers', metadata,
       Column('customer_key', Integer, primary_key=True),
       Column('gender', String, nullable=False),
       Column('customer_name', String, nullable=False),
       Column('customer_city', String, nullable=False),
       Column('state_code', String, nullable=False),
       Column('customer_state', String, nullable=False),
       Column('zip_code', String, nullable=False),
       Column('country', String, nullable=False),
       Column('continent', String, nullable=False),
       Column('customer_birthday', Date, nullable=False)
    )

    exchange_rates_table = Table('cp2_exchange_rates', metadata,
       Column('currency', String, primary_key=True),
       Column('exchange_rate', Float, nullable=False),
       Column('exchange_rate_date', Date, primary_key=True)
    )

    sales_table = Table('cp2_sales', metadata,
       Column('order_number', Integer, primary_key=True),
       Column('line_item', Integer, primary_key=True),
       Column('customer_key', Integer, ForeignKey('cp2_customers.customer_key'), nullable=False),
       Column('store_key', Integer, ForeignKey('cp2_stores.store_key'), nullable=False),
       Column('product_key', Integer, ForeignKey('cp2_products.product_key'), nullable=False),
       Column('quantity', Integer, nullable=False),
       Column('currency_code', String, nullable=False),
       Column('order_date', Date, nullable=False),
       Column('delivery_date', Date, nullable=True),
       ForeignKeyConstraint(
        ["currency_code", "order_date"], ["cp2_exchange_rates.currency", "cp2_exchange_rates.exchange_rate_date"]
       )
    )
    
    db = create_engine(conn_string)
    
    connection = db.connect()
    
    print('Drop tables....')
    metadata.drop_all(db)
    
    print('Creating tables...')
    metadata.create_all(db)
    
    return connection

def load_stores(connection):
    # Load the stores data into dataframe
    stores_df = pd.read_csv('datasets/Stores.csv')

    # Create a date column to hold "open date"
    stores_df['Formatted Open Date'] = pd.to_datetime(stores_df['Open Date'], format='%m/%d/%Y')

    # Rename dataframe column names to be DB friendly
    renamed_stores_df = stores_df.rename(columns={'StoreKey':'store_key', 'Country':'store_country', 'State':'store_state', 'Square Meters':'store_size', 'Formatted Open Date': 'store_open_date'})

    # Load the store data into database
    renamed_stores_df[['store_key','store_country','store_state','store_size','store_open_date']].to_sql('cp2_stores', connection, if_exists='append', index=False)

def load_products(connection):
    # Load the products data into dataframe
    products_df = pd.read_csv('datasets/Products.csv')

    # Convert the amount fields to decimal
    for x in ['Unit Price USD', 'Unit Cost USD']:
        products_df['t' + x] = products_df[x].apply(lambda x: x[1:].strip().replace(',','')).astype(float)

    # Rename columns to make it DB friendly
    renamed_products_df = products_df.rename(columns={'ProductKey':'product_key', 'Product Name':'product_name', 'Brand':'product_brand', 'Color':'product_color', 'tUnit Price USD': 'unit_price', 'tUnit Cost USD': 'unit_cost', 'SubcategoryKey':'sub_category_key','Subcategory':'sub_category','CategoryKey':'category_key','Category':'product_category'})

    # Drop unformatted columns inplace
    renamed_products_df.drop(['Unit Price USD','Unit Cost USD'], inplace=True, axis=1)
    
    # Load data into tables
    renamed_products_df.to_sql('cp2_products', connection, if_exists='append', index=False)

def load_customers(connection):

    # Load the customer data into dataframe
    customers_df = pd.read_csv('datasets/Customers.csv', encoding='unicode_escape')
    
    # Update the state code for Napoli
    customers_df.loc[customers_df['State'] == 'Napoli', 'State Code'] = 'NA'
    
    # Create a new Date column to hold birthday
    customers_df['fBirthday'] = pd.to_datetime(customers_df['Birthday'], format='mixed', dayfirst=True)
    
    # Drop the text Birthday column from dataframe
    customers_df.drop(['Birthday'], inplace=True, axis=1)
    
    # Rename columns to be more database friendly
    renamed_customers_df = customers_df.rename(columns={'CustomerKey':'customer_key', 'Gender':'gender', 'Name':'customer_name', 'City':'customer_city', 'State Code': 'state_code', 'State': 'customer_state', 'Zip Code':'zip_code','Country':'country','Continent':'continent','fBirthday':'customer_birthday'})
    
    # Load data into tables
    renamed_customers_df.to_sql('cp2_customers', connection, if_exists='append', index=False)

def load_exchange_rates(connection):

    # Load the exchange rate data into dataframe
    exchange_rates_df = pd.read_csv('datasets/Exchange_Rates.csv', encoding='unicode_escape')
    
    # Create a new date column to hold the exchange rate date
    exchange_rates_df['fDate'] = pd.to_datetime(exchange_rates_df['Date'], format='%m/%d/%Y')
    
    # Rename columns to be more DB friendly
    renamed_exchange_rates_df = exchange_rates_df.rename(columns={'fDate': 'exchange_rate_date', 'Currency': 'currency', 'Exchange': 'exchange_rate'})
    
    # Remove the unformatted string date column
    renamed_exchange_rates_df.drop(['Date'], inplace=True, axis=1)
    
    # Load data into tables
    renamed_exchange_rates_df.to_sql('cp2_exchange_rates', connection, if_exists='append', index=False)

def load_sales(connection):
    sales_df = pd.read_csv('datasets/Sales.csv')
    
    sales_df['fOrder Date'] = pd.to_datetime(sales_df['Order Date'], format='%m/%d/%Y')
    sales_df['fDelivery Date'] = pd.to_datetime(sales_df['Delivery Date'], format='%m/%d/%Y')
    
    renamed_sales_df = sales_df.rename(columns={'Order Number':'order_number','Line Item':'line_item','fOrder Date':'order_date','fDelivery Date':'delivery_date','CustomerKey':'customer_key','StoreKey':'store_key','ProductKey':'product_key','Quantity':'quantity','Currency Code':'currency_code'})
    
    renamed_sales_df.drop(['Order Date','Delivery Date'], inplace=True, axis=1)

    renamed_sales_df.to_sql('cp2_sales', connection, if_exists='append', index=False)

def main():
    connection = get_connection()
    
    print('Loading stores data...')
    load_stores(connection)

    print('Loading products data...')
    load_products(connection)

    print('Loading customers data...')
    load_customers(connection)

    print('Loading exchange rate data...')
    load_exchange_rates(connection)

    print('Loading sales data...')
    load_sales(connection)

if __name__ == "__main__":
    main()

