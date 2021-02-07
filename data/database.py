# Import libraries
import pandas as pd
import psycopg2
from config.config import get_db_config_vals
from sqlalchemy import create_engine,types, inspect
from sqlalchemy.sql import text
from datetime import date

#Database class for managing database connection
class Database:
    #Constructs the engine upon initialization, connect to the database
    def __init__(self,user,password,host,port,dbname):
        self.engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s'%(user,password,host,port,dbname))

    def get_connection(self):
        return self.engine.connect()
    #Returns True if database has table with name
    def has_table(self, name):
        return self.engine.has_table(name)

    #Uploads DataFrame to the database, creates new table with tablename
    def create_therapists_table_from_df(self, df, tablename):
        df.to_sql(tablename,con=self.engine, schema=None, index=False, if_exists='replace',
        dtype = {'createdTime':types.String,'fields.Имя':types.String,'fields.PhotoId':types.String,'fields.Методы':types.String})

    #Uploads DataFrame to the database, creates new table with tablename
    def create_table_from_df(self,df,tablename):
        df.to_sql(tablename,con=self.engine, schema=None, index=False, if_exists='replace')

    #Selects all data from given table, returns it as DataFrame
    def get_df_from_table(self,tablename):
        conn = self.engine.connect() #get the connection obj
        query = "SELECT * FROM "+tablename+";"
        df = pd.read_sql(query,conn)
        conn.close()
        return df
   
    #appends dataframe to the table  
    def insert_df(self, table, dataframe):
        conn = self.engine.connect()
        dataframe.to_sql(table, con=self.engine, if_exists='append', index=False)
        conn.close()

    #
    def delete(self,table,id_col,t_id):
        with self.engine.connect() as connection:
            result = connection.execute(text("DELETE FROM "+table+" WHERE "+id_col +"='" +t_id+"';"))
            connection.close()
        #print(result.rowcount)

    def close(self):
        self.engine.dispose()

# Take in a PostgreSQL connection and a table name, creates a table
def create_table_from_dataframe(conn, username, password, host, port,tablename, airflow_table_df):
    airflow_table_df['ДатаЗапуска']=datetime.now()
    
    try:
        engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s/%s'%(username,password,host,port,tablename))
        airflow_table_df.to_sql(tablename,con=engine, schema=None, index=True, if_exists='append',
        dtype = {'createdTime':types.DateTime,'fields.Имя':types.String,'fields.Фотография':types.JSON,'fields.Методы':types.ARRAY(types.String)})
    except Exception as e:
        print(e)
    engine.dispose()


