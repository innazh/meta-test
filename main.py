# Import libraries
import pandas as pd
import requests
from config.config import get_db_config_vals,get_airtable_config_vals
from data.database import Database,create_table_from_dataframe
from airflow_service import get_airflow_table, parse_table

#Function that synchronizes the inserts and updates of the airtable with our db
#note: code will break if the id field of the table is not 'id'. The function will server our current purposes fine though.
def update_curr_table_with_new_recs(db, airflow_df, db_df, table_name, id_col_name):
    #compare with data from airtable
    #for inserts:"Find Rows in airflow_df Which Are Not Available in db_df"
    newrows = airflow_df.merge(db_df,how = 'outer', left_index=False, right_index=False,indicator=True).loc[lambda x : x['_merge']=='left_only']
    if not newrows.empty: #if row was added or updated
            newrows = newrows.drop('_merge',axis=1)
            print(newrows)
            #check if any of the detected records already exist in the current_dataframe, remove them if they do.
            for i,row in newrows.iterrows():
                res = db_df.isin([row[0]])
                if res.any()[id_col_name]:
                    db.delete(table_name, id_col_name,row[0])
                print(res.any()[0])
            db.insert_df(table_name, newrows)
    else:
        print("No records to update or insert in table "+table_name)
    return

#Performs a merge on the dataframes removes the records that are present in the database but aren't present in the airflow
def remove_old_recs(db, airflow_df, db_df, table_name, id_col_name):
    # find rows in curr_therapists_df which are nt present in therapists_df
    del_rows=airflow_df.merge(db_df, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='right_only']
    if not del_rows.empty: #if there are rows to delete
            for i,row in del_rows.iterrows():
                db.delete(table_name, id_col_name, row[0])
    else:
        print("No records to delete in table " + table_name)
    return

def main():
    airflow_params = get_airtable_config_vals('database.ini')
    #get airflow data as DataFrame
    airflow_data = get_airflow_table(airflow_params['url'],airflow_params['api_key'])
    therapists_df,photos_df,thumbnails_df = parse_table(airflow_data)
    
    #parse params (config)
    params = get_db_config_vals('database.ini')
    #connect to db
    db = Database(params['user'],params['password'],params['host'],params['port'],params['database']) 
    #if main table doesn't exist then just copy airtable data to our db
    if not db.has_table('psychotherapists'):
        db.create_therapists_table_from_df(therapists_df,'psychotherapists')
        db.create_table_from_df(photos_df,'photos')
        db.create_table_from_df(thumbnails_df,'thumbnails')
    #else - compare the data
    else:
        #get current dataframes from database
        curr_therapists_df = db.get_df_from_table('psychotherapists')
        curr_photos_df = db.get_df_from_table('photos')
        curr_thumbnails_df = db.get_df_from_table('thumbnails')

        #compare with airtable and synchronize - psychotherapists
        update_curr_table_with_new_recs(db, therapists_df, curr_therapists_df, 'psychotherapists', 'id')#works on its own
        curr_therapists_df = db.get_df_from_table('psychotherapists') #refresh the curr df
        remove_old_recs(db,therapists_df, curr_therapists_df, 'psychotherapists', 'id')
        #photos
        update_curr_table_with_new_recs(db, photos_df, curr_photos_df, 'photos', 'id')#works on its own
        curr_photos_df = db.get_df_from_table('photos') #refresh the curr df
        remove_old_recs(db,photos_df, curr_photos_df, 'photos', 'id')
        #thumbnails
        update_curr_table_with_new_recs(db, thumbnails_df, curr_thumbnails_df, 'thumbnails', 'photo_id')#works on its own
        curr_thumbnails_df = db.get_df_from_table('thumbnails') #refresh the curr df
        remove_old_recs(db,thumbnails_df, curr_thumbnails_df, 'thumbnails', 'photo_id')
    db.close()
    return


if __name__ == "__main__":
    main()

# def convertJSONtoPandasDf(json_api_data):
#     # here we convert the data we got from the api to the data in our postgre db
#     df = pd.json_normalize(json_api_data)  # import pandas as pd
#     # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table
