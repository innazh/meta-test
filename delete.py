# # Import libraries
# import pandas as pd
# import psycopg2

# from config.config import config

# # Connect to PostgreSQL
# params = config(config_db='database.ini')
# engine = psycopg2.connect(**params)
# print('Python connected to PostgreSQL!')
# # Delete rows from the table
# cur = con.cursor()
# cur.execute("""
# DELETE FROM customer WHERE customer_id = 12345;
# """)
# print('Values deleted from PostgreSQL')
# # Close the connection
# con.commit()
# con.close()
