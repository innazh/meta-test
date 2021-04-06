# Import libraries
import pandas as pd
import psycopg2
from config.config import get_db_config_vals
from sqlalchemy import create_engine,types, inspect
from sqlalchemy.sql import text
from datetime import datetime
import json

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
    def create_table_from_df(self, df, tablename):
        index_flag=False
        if df.index.name=="id":
            index_flag = True
        df.to_sql(tablename, con=self.engine, schema=None, index=index_flag, if_exists='replace')

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

    #deletes row with id=t_id where id_col is the name of the primary key column from the table
    def delete(self,table,id_col,t_id):
        with self.engine.connect() as connection:
            result = connection.execute(text("DELETE FROM "+table+" WHERE "+id_col +"='" +t_id+"';"))
            connection.close()
        #print(result.rowcount)

    #function that runs every time we run this script and 
    def insert_raw_data(self,data):
        #Create table if it does not exist
        conn = self.engine.connect()
        conn.execute(text("""CREATE TABLE IF NOT EXISTS raw_data(id SERIAL PRIMARY KEY,
                                                                date TEXT,
                                                                data TEXT);"""))
        conn.close()
        #create dataframe and populate it with data
        d = [[datetime.now(),data]]
        df = pd.DataFrame(d,columns=['date','data'])
        df['data'] = list(map(lambda x: json.dumps(x), df['data']))
        #insert it to the db
        df.to_sql('raw_data',con=self.engine, schema=None, index=False, if_exists='append',
        dtype = {'data':types.String,'date':types.String})

    #manage keys
    #Accepts the name of the talbe for which the PK needs to be set and a list of PR column names
    #Executes ALTER TABLE command on the db connaction
    def set_primary_key(self, table, pk_cols):
        con = self.engine.connect()
        q = 'alter table ' + table + ' add primary key('
        for i, col in enumerate(pk_cols):
            q+= col + ','
        q = q[:-1]
        q+=');'
        print(q)
        con.execute(q)
        con.close()

    #Accepts the name of the talbe for which the FK needs to be set, name of the FK column, 
    #name of the referenced table and the referenced column name
    #Executes ALTER TABLE command on the db connaction
    def set_foreign_key(self, table, fk_col, ref_table, ref_col):
        con = self.engine.connect()
        q = 'ALTER TABLE ' + table + ' ADD FOREIGN KEY(' + fk_col + ')' + ' REFERENCES ' + ref_table + ' (' + ref_col + ') ' + 'on delete cascade;' 
        print(q)
        con.execute(q)
        con.close()
    #https://stackoverflow.com/questions/17325006/how-to-create-a-foreignkey-reference-with-sqlalchemy
    #https://www.fullstackpython.com/sqlalchemy-schema-createtable-examples.html


    def close(self):
        self.engine.dispose()


