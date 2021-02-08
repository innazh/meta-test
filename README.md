# Тестовое задание для Junior Full Stack Разработчика в Мету - completed
## CLI-script
Содержит Docker файл для того, чтобы запустить PostgreSQL БД и python скрипт по выгрузке данных из airtable.
Скрипт создает следующие таблицы в БД: psychotherapists, photos, thumbnails и raw_data. Таблицы содержат данные о психотерапевтах, фото, миниатюрах 
и сырых данных соответственно.
При повторном запуске, скрипт синхронизирует таблицы в БД с airtable.
## front-end
Содержит Django проект, который устанавливает соединение с нашей БД в докер контейнере. Django приложение отображает данные, находящиеся в нашей БД, 
по URL /psychotherapists и /psychotherapists/id (где id - это id психотерапевта в БД).
### Структура отсутствующего файла config.ini в папке CLI-script/config/
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
