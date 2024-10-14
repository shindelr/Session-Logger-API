from os import getenv
from dotenv import load_dotenv
import pymssql

load_dotenv()
password = getenv("DB_PASSWORD")
user = getenv("DB_USER")
db = getenv("DB_NAME")
server = getenv("DB_SERVER")

print(password, user, db, server)
conn = pymssql.connect(server, user, password, db)
cursor = conn.cursor()

query = "select * from Tide"
cursor.execute(query)
print(cursor.fetchall())
