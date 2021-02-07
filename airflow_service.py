import requests
import pandas as pd

#Sends a GET request to airtable API, returns JSON data
def get_airflow_table(url, key):
    headers = {'Authorization': 'Bearer %s'%key}
    r = requests.get(url=url, headers=headers)
    data = r.json()
    data = data['records']
    return data

#parses jSON data into dataframes, done according to one of the main design principles in software dev - KISS
def parse_table(data):
    table = pd.json_normalize(data)
    therapists_df = table.drop('fields.Фотография',axis=1)
    #add photo id column in our dataframe - table normalization process. Breaking up the main table into smaller ones - psychotherapists and photo table
    therapists_df['fields.PhotoId']=''
    #turn therapists_df['fields.Методы'] of type list to a string
    t=0
    for methods in therapists_df['fields.Методы']:
        s=""
        for method in methods:
            s+=method+","
        therapists_df['fields.Методы'][t]=s[:len(s)-1]
        t+=1

    i=0
    for row in table['fields.Фотография']:
        therapists_df['fields.PhotoId'][i]=row[0]['id'] #assumption: a therapist always has one picture only
        i=i+1
    
    #create photo table from 'fields.Фотография' column
    photos_table = pd.concat([pd.DataFrame(pd.json_normalize(x, max_level=0)) for x in table['fields.Фотография']],ignore_index=True)
    #drop thumbnails column from photos dataframe as it'll be a separate table
    photos_df = photos_table.drop('thumbnails',axis=1)
    
    #create a new df - for thumbnails
    thumbnails_df = pd.DataFrame(columns=['photo_id','type','url','width','height'])
    
    k=0
    for row in photos_table['thumbnails']:
        photo_id = photos_df['id'][k]
        #print(row)
        for key in row:
            url = row[key]['url']
            width = row[key]['width']
            height = row[key]['height']
            #insert row into thumbnail df
            r = {'photo_id':photo_id,'type':key,'url':url,'width':width,'height':height}
            thumbnails_df = thumbnails_df.append(r,ignore_index=True)
        k=k+1

    return therapists_df, photos_df,thumbnails_df


