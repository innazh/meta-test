from parser.data_parser import create_specialisation_table

#Accepts a database object, airtable dataframe, data frame with current data from our db, and the pk column name
#Function that synchronizes the inserts and updates of the airtable with our db
def get_new_or_changed_records(db, airflow_df, db_df, table_name):
    newrows = airflow_df.merge(db_df, how = 'outer', left_index=False, right_index=False,indicator=True).loc[lambda x : x['_merge']=='left_only']
    if not newrows.empty:
            newrows = newrows.drop('_merge',axis=1)
    return newrows


#Performs a merge on the dataframes removes the records that are present in the database but aren't present in the airtable
def remove_records(db, airflow_df, db_df, table_name, id_col_name):
    # find rows in curr_therapists_df which are nt present in therapists_df
    del_rows = airflow_df.merge(db_df, how = 'outer' ,indicator=True).loc[lambda x : x['_merge']=='right_only']
    
    if not del_rows.empty: #if there are rows to delete
            for i,row in del_rows.iterrows():
                db.delete(table_name, id_col_name, row[0])
    else:
        print("No records to delete in table " + table_name)
    
    return

def delete_rec_if_present(db, rows, curr_df, id_col_name, table_name):
#check if any of the detected records already exist in the current_dataframe, remove them if they do.
    for i,row in rows.iterrows():
        res = curr_df.isin([row[0]])
        if res.any()[id_col_name]:
            db.delete(table_name, id_col_name,row[0])
            print("Delete from "+table_name+", row="+row[0]+", col name = " + id_col_name)

def handle_psychotherapist(db, therapists_df, curr_therapists_df):
    t_changed = get_new_or_changed_records(db, therapists_df, curr_therapists_df, 'psychotherapist')
    delete_rec_if_present(db, t_changed, curr_therapists_df, 'id', 'psychotherapist')
    db.insert_df('psychotherapist', t_changed)

    return t_changed['id'].values    

def handle_photo(db, photo_df, curr_photo_df):
    changed = get_new_or_changed_records(db, photo_df, curr_photo_df, 'photo')
    delete_rec_if_present(db, changed, curr_photo_df, 'id', 'photo')
    db.insert_df('photo', changed)     

def handle_thumbnail(db, t_df, curr_t_df):
    changed = get_new_or_changed_records(db, t_df, curr_t_df, 'thumbnail')
    delete_rec_if_present(db, changed, t_df, 'photo_id', 'thumbnail')
    next_id = len(curr_t_df)+1

    #assign ids before the insert:
    for i in range(len(changed)):
        changed['id'].loc[i]=next_id
        next_id+=1
    db.insert_df('thumbnail', changed) 

def handle_approach(db, a_df, curr_a_df):
    changed = get_new_or_changed_records(db, a_df, curr_a_df, 'approach')
    delete_rec_if_present(db, changed, curr_a_df, 'name', 'approach')
    next_id = len(curr_a_df)+1

    # #assign ids before the insert:
    for i in range(len(changed)):
        changed['id'].loc[i]=next_id
        next_id+=1
    db.insert_df('approach', changed) 

#updated psychotherapists ids: list
#a list of approaches handled by every therapist
#1. Clean every entry for p_ids that are contained here.
#2. Pair the data and insert these pairs inside the db
def handle_specialisation(db, api_service, p_ids, curr_a_df):
    approaches = []
    if not p_ids:
        return
    #delete
    for p_id in p_ids:
        print("Delete " + p_id + " from specialisation table")
        approaches.append(api_service.get_approaches(p_id))
        db.delete("specialisation","p_id",p_id)
    #pair
    df = create_specialisation_table(approaches, p_ids, curr_a_df) 
    df['a_id']= df['a_id']+1 #increment all ids by 1, for some reason the ids contained by df are all one below the actual ones
    print(df)
    db.insert_df('specialisation', df) 