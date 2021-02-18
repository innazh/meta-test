## CLI-script
Contains a Docker file to run PostgreSQL database. 
The script uses airtable API to get data from the remote table, parses & normalizes it, then inserts it into the database.
Script synchronizes the data base with the data from airtable upon any consequent runs.
## front-end
Contains a Django project, which renders the data from the database - the data we collected through the CLI script. It's just a simple mock-up, nothing fancy; my first experience using Django.
### The structure of a missing file in config.ini, in folder CLI-script/config/
````python
[postgresql]
host = localhost
database = psychotherapists
user = postgres
password = password
port = 5432

[airtable]
api_key = api_key_val
base_ID = base_ID_val
url = full_url_w_tablename
table_name = tablename
````
