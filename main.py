# Import libraries
from config.config import get_project_root
from src.data.db_conn import load_db_table

# Project root
PROJECT_ROOT = get_project_root()
# Read database - PostgreSQL
df = load_db_table(config_db='database.ini',
                   query='SELECT * FROM tablename LIMIT 5')
print(df)


def main():
    print("hi")


if __name__ == "__main__":
    main()
