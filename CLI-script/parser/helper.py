
import pandas as pd

#Finds value within a data frame and returns its index
#Past: accepts a method and returns the id of that method in the dataframe
#Accepts data frame, a value to search for, start index and the name of the column to search under
def find_value(df, val, column, start):
    _id = -1
    for i in range(start, len(df.index.values)+1):
        if df.loc[i][column]==val:
            _id=i
            break
    return _id

#Accepts a dataframe, column name and an array of values
#adds new column to the data frame and assigns the values in the array to it
def add_column(df, name, arr):
    df[name]=''
    for i in range(len(arr)):
        df[name].loc[i]=arr[i]
    return df

#accepts a data frame and a column name the values of which need to be extracted
#goes row by row through the dataframe and appends the values under the selected column to a list
#returns a the column represented by a list of values
def extract_methods(p_df):
    methods = []
    for i in range(p_df['fields.Методы'].size): #number of rows in Methods column
        methods.append((p_df['fields.Методы'].loc[i]))
    return methods

#TODO:
def unpack_psychotherapists():
    return
#TODO:
def unpack_photo_stuff():
    return

#unpacks the initial json data and breaks it down into separate data frames: therapists_df, photos_df, photo_data_part and an array of methods
def unpack_data(data):
    data_df = pd.json_normalize(data)
    #start normalization process for psychotherapists
    therapists_df = data_df.drop('fields.Фотография',axis=1) #separate the photo table from the data table
    methods = extract_methods(therapists_df) #extract methods   
    #Remove methods(approaches) column from psychotherapists table:
    therapists_df.drop('fields.Методы', axis=1, inplace=True)#axis=1 = column, 0 = row

    #create photo table from 'fields.Фотография' column in the original data frame
    photo_data_part = pd.concat([pd.DataFrame(pd.json_normalize(x, max_level=0)) for x in data_df['fields.Фотография']], ignore_index=True)
    photos_df = photo_data_part.drop('thumbnails',axis=1) #drop thumbnails from photo part of the table to get the photos dataframe
    
    return therapists_df, photos_df, photo_data_part, methods


