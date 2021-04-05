import pandas as pd

#unpacks the initial json data and breaks it down into separate data frames: therapists_df, photos_df, photo_data_part
def unpack_data(data):
    data_df = pd.json_normalize(data)#initial data frame that contains the entire table pd.DataFrame.from_dict(data)
    therapists_df = data_df.drop('fields.Фотография',axis=1) #separate the photo table from the data table

    #create photo table from 'fields.Фотография' column in the original data frame
    photo_data_part = pd.concat( [pd.DataFrame(pd.json_normalize(x, max_level=0)) for x in data_df['fields.Фотография'] ], ignore_index=True)

    photos_df = photo_data_part.drop('thumbnails',axis=1) #drop thumbnails from photo part of the table to get the photos dataframe

    return therapists_df, photos_df, photo_data_part

#Accepts an array of methods(approaches) used by psychotherapists
#Returns approaches dataframe that contains id and name columns
def create_approaches_table(methods):
    approaches_df = pd.DataFrame(columns=['id','name'])
    pk_id = 1
    for row in methods:
        for method in row:
            df = pd.DataFrame([[pk_id, method]], columns=['id', 'name'])
            if not approaches_df.isin([method]).any().any():
                approaches_df = approaches_df.append(df)
                pk_id+=1
    approaches_df = approaches_df.set_index('id')
    return approaches_df

#TODO: possibly conver into a more general function.
#Currently: accepts a method and returns the id of that method in the dataframe
def find_method_id(method, df):
    _id = -1
    for i in range(1, len(df.index.values)+1):
        if df.loc[i]['name']==method:
            _id=i
            break
    return _id

#Accepts a 2d list of methods, a dataframe for approaches(methods) table and a data frame for therapists table
#Return a dataframe "specialisation" - table that links therapists and approaches tables together
def create_specialisation_table(methods, therapists_df, approaches_df):
    specialisation_df = pd.DataFrame(columns=['p_id', 'a_id'])

    for i, row in enumerate(methods):
        p_id = therapists_df['id'].loc[i]
        for method in row:
            a_id = find_method_id(method, approaches_df)
            df = pd.DataFrame([[p_id, a_id]], columns=['p_id', 'a_id'])
            specialisation_df = specialisation_df.append(df)

    return specialisation_df

# acts as a helper function to 'create_thumbnails_table', accepts all the data necessary to create a row for thumbnail table and returns a dataframe of this row
def create_thumbnail_row(pk_id, photoId, element, t_type):
    df = pd.DataFrame([[pk_id, t_type, element['url'], element['width'], element['height'], photoId]], columns=['id', 'type', 'url', 'width', 'height', 'photo_id'])
    return df

#Creates a thumbnails table.
def create_thumbnails_table(data):
    arr = data['thumbnails'].values
    thumbnails_df = pd.DataFrame(columns=['id', 'type', 'url', 'width', 'height', 'photo_id'])

    pk=1
    for i, elem in enumerate(arr):
        small_df = create_thumbnail_row(pk, data['id'].loc[i], elem['small'], "small")
        pk+=1
        large_df = create_thumbnail_row(pk, data['id'].loc[i], elem['large'], "large")
        pk+=1
        full_df =  create_thumbnail_row(pk, data['id'].loc[i], elem['full'], "full")
        pk+=1
        thumbnails_df = pd.concat([thumbnails_df, small_df, large_df, full_df]) #the docs say concat is more efficient than a iterative append
    
    thumbnails_df = thumbnails_df.set_index('id')

    return thumbnails_df

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
#TODO: make this a generic function that works for extracting any data under a certain column in the data frame, although it's actually fine for my current purposes
def extract_methods(p_df):
    methods = []
    for i in range(p_df['fields.Методы'].size): #number of rows in Methods column
        methods.append((p_df['fields.Методы'].loc[i]))
    return methods

#process all records/rows at once
def process_bulk(data):
    #1. unpack the data from the original data frame
    therapists_df, photos_df, photo_thumbnails_df = unpack_data(data)
    #2. start normalization process for psychotherapists
    #2.1 Remove a transitive dependency - methods from psychotherapists table:
    #2.1.1 Extract methods
    methods = extract_methods(therapists_df)
    #2.1.2 Remove methods(approaches) column from psychotherapists table:
    therapists_df = therapists_df.drop('fields.Методы', axis=1) #axis=1 = column, 0 = row
    #2.1.3 Create a separate table - approaches, that'll contain all approaches available
    approaches_df = create_approaches_table(methods)
    #2.1.4 Create another table - specialisation, which will establish a relationship between psychotherapists and approaches they use.
    specialisation_df = create_specialisation_table(methods, therapists_df, approaches_df)

    #2.2 Start normalization for the photos and thumbnails tables
    #2.2.1 - Create thumbnails table.
    thumbnails_df = create_thumbnails_table(photo_thumbnails_df)
    #2.2.2 - Add a psychotherapists_id column to photos table, which will act as a foreign key.
    photos_df = add_column(photos_df, "p_id", therapists_df['id'])
    #insert the dataframes into the db
