import pandas as pd
import numpy as np
import requests
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait


s=Service("C:/Users/ad681/OneDrive/Desktop/chromedriver.exe")

driver=webdriver.Chrome(service = s)
driver.get("https://www.malabargoldanddiamonds.com/catalogsearch/result/?q=jewellery")
time.sleep(3)
for i in range(10):
    driver.execute_script('window.scrollTo(0,document.body.scrollHeight)')
    time.sleep(5)
    try:
        show_more_button = WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Show More']")))
        show_more_button.click()
    except:
        pass
    time.sleep(3)
temp=driver.page_source
driver.close()

page=BeautifulSoup(temp)
soup=page

products = soup.find_all('div', 'image_wrapper')
product_url = []
image_url = []
name = []
prices = []
category = []
k = 0
for i in products:
    try:
        image_url.append(i.find('img').get('data-src'))
    except:
        image_url.append(np.nan)
    try:
        product_url.append(i.find('a').get('href'))
    except:
        product_url.append(np.nan)
    try:
        name.append(i.find('img').get('title'))
    except:
        name.append(np.nan)
        temp = soup.find_all('div', 'image_wrapper')[k].find('a').get('href')
    try:
        temp = i.find('a').get('href')
        page = requests.get(temp)
        soup1 = BeautifulSoup(page.content, 'html.parser')
        prices.append(soup1.find_all('div', 'price-box list-regular')[0].find('span', 'price').text[2:])
    except:
        prices.append(np.nan)
    try:
        temp = i.find('a').get('href')
        page = requests.get(temp)
        soup1 = BeautifulSoup(page.content, 'html.parser')
        category.append(soup1.find('div', 'f-left col-xs-16 col-md-16').find('ul').find('span',
                                                                                        'f-right col-md-8 product_type').text)
    except:
        category.append(np.nan)

    print(k)
    k += 1


jewellery = pd.DataFrame(
    {"Product_URL": product_url, "Image_URL": image_url, "Name": name, "Prices": prices, "Category": category})

def table_creation():
    import mysql.connector
    from mysql.connector import Error

    try:
        # Establish the connection to the MySQL database
        connection = mysql.connector.connect(
            host='localhost',        # Replace with your host
            database='Ajay',# Replace with your database name
            user='root',    # Replace with your MySQL username
            password='Das45678' # Replace with your MySQL password
        )

        if connection.is_connected():
            print("Connected to MySQL database")

            # Define the SQL query to create a new table
            create_table_query = """
            CREATE TABLE jewellery (
                product_url VARCHAR(255),
                image_url VARCHAR(255),
                name_ VARCHAR(255),
                price VARCHAR(100),
                category VARCHAR(100)
            );
            """

            # Create a cursor object
            cursor = connection.cursor()

            # Execute the SQL query to create the table
            cursor.execute(create_table_query)

            print("Table 'jewellery' created successfully.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            # Close the cursor and connection
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


def insert_data(product_url,image_url,name,price,category):
    import mysql.connector
    from mysql.connector import Error

    try:
        # Establish the connection to the database
        connection = mysql.connector.connect(
            host='localhost',        # Replace with your host
            database='Ajay',# Replace with your database name
            user='root',    # Replace with your MySQL username
            password='Das45678' # Replace with your MySQL password
        )

        if connection.is_connected():
            print("Connected to MySQL database")
            # Define the SQL query to insert data
            insert_query = """
            INSERT INTO jewellery (product_url, image_url, name_,price,category)
            VALUES (%s, %s, %s, %s, %s)
            """

            # Data to be inserted
            data_to_insert = (product_url,image_url,name,price,category)

            # Create a cursor object
            cursor = connection.cursor()

            # Execute the SQL query
            cursor.execute(insert_query, data_to_insert)

            # Commit the transaction
            connection.commit()

            print(f"{cursor.rowcount} record(s) inserted successfully into {cursor.lastrowid}.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")

    finally:
        if connection.is_connected():
            # Close the cursor and connection
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

table_creation()
for i in range(jewellery.shape[0]):
    insert_data(jewellery.iloc[i]['Product_URL'],jewellery.iloc[i]['Image_URL'],jewellery.iloc[i]['Name'],jewellery.iloc[i]['Prices'],jewellery.iloc[i]['Category'])