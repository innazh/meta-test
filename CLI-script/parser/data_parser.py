import pandas as pd
from parser.helper import unpack_data, find_value, add_column, extract_methods

#Accepts a dataframe that contains the entire table in its initial format,
#the frame undergoes a normalisation process which results in
#Returnes value: 5 distinct data frames 
def process_bulk(data):
    therapists_df, photos_df, photo_thumbnails_df, methods = unpack_data(data)

    #2.1.3 Create a separate table - approaches, that'll contain all treatment approaches available
    approaches_df = create_approaches_table(methods)
    #2.1.4 Create another table - specialisation, which will establish a relationship between psychotherapists and approaches they use.
    specialisation_df = create_specialisation_table(methods, therapists_df['id'].values, approaches_df)
    #2.2 Rename the name column in therapists table
    therapists_df.rename(columns={"fields.Имя":"name"}, inplace=True)
    #2.3 Start normalization for the photos and thumbnails tables
    #2.3.1 - Create thumbnails table.
    thumbnails_df = create_thumbnails_table(photo_thumbnails_df)
    #2.3.2 - Add a psychotherapists_id column to photos table, which will act as a foreign key.
    photos_df = add_column(photos_df, "p_id", therapists_df['id'])
    # print(therapists_df)
    # print(photos_df)
    # print(approaches_df)
    # print(specialisation_df)
    # print(thumbnails_df)

    return therapists_df, photos_df, approaches_df, specialisation_df, thumbnails_df

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

#Accepts a 2d list of methods, a dataframe for approaches(methods) table and a data frame for therapists table
#Return a dataframe "specialisation" - table that links therapists and approaches tables together
def create_specialisation_table(methods, therapists_ids, approaches_df):
    specialisation_df = pd.DataFrame(columns=['p_id', 'a_id'])

    for i, row in enumerate(methods):
        p_id = therapists_ids[i]
        for method in row:
            a_id = find_value(approaches_df, method, 'name', 1)
            df = pd.DataFrame([[p_id, a_id]], columns=['p_id', 'a_id'])
            specialisation_df = specialisation_df.append(df)

    return specialisation_df

# acts as a helper function to 'create_thumbnails_table', accepts all the data necessary to create a row for thumbnail table
# returns a dataframe of this row
def create_thumbnail_row(pk_id, photoId, element, t_type):
    df = pd.DataFrame([[pk_id, t_type, element['url'], element['width'], element['height'], photoId]], columns=['id', 'type', 'url', 'width', 'height', 'photo_id'])
    return df

#Accepts an array if thumbmails from a part of the photo dataframe, and array containing photoIds
#Constructs a thumbnails dataframe.
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
        thumbnails_df = pd.concat([thumbnails_df, small_df, large_df, full_df]) #the docs say concat is more efficient than iterative append
    
    thumbnails_df = thumbnails_df.set_index('id')

    return thumbnails_df