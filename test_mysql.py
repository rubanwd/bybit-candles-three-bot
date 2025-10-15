# test_mysql.py (положи рядом с main.py)
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()
url = os.getenv("DATABASE_URL")
print("Using:", url)

engine = create_engine(url, pool_pre_ping=True)
with engine.connect() as con:
    print(con.execute(text("SELECT 1")).scalar())
    con.execute(text("CREATE TABLE IF NOT EXISTS ping(id INT PRIMARY KEY AUTO_INCREMENT, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"))
    con.execute(text("INSERT INTO ping() VALUES ()"))
    con.commit()
print("OK")
