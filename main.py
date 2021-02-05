# Import libraries
import pandas as pd
import requests

# from config.config import get_project_root
# from src.data.db_conn import load_db_table

URL = "https://api.airtable.com/v0/appBzMKXpuuvu1Zlt/Psychotherapists"

# # Project root
# PROJECT_ROOT = get_project_root()
# # Read database - PostgreSQL
# df = load_db_table(config_db='database.ini',
#                    query='SELECT * FROM tablename LIMIT 5')
# print(df)


def main():
    GETreq()
    print("hi")


def GETreq():
    headers = {'Authorization': 'Bearer key0AKoGtvg8GlrvF'}
    r = requests.get(url=URL, headers=headers)
    data = r.json()
    print(data)


def convertJSONtoPandasDf(json_api_data):
    # here we convert the data we got from the api to the data in our postgre db
    df = pd.json_normalize(json_api_data)  # import pandas as pd
    # https://stackoverflow.com/questions/23103962/how-to-write-dataframe-to-postgres-table


def compareDataframes(db_df, api_df):
    # https://datatofish.com/compare-values-dataframes/
    # https://sqlity.net/en/106/the-underestimated-complexity-of-a-table-compare-algorithm/


if __name__ == "__main__":
    main()
