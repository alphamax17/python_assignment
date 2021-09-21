from flask import Flask, request, jsonify, render_template
import sqlalchemy as db
from sqlalchemy import Table, Column, Integer, String, MetaData, VARCHAR
from sqlalchemy import text
import psycopg2
import logging
import random
import json
from string import ascii_letters

_logger = logging.getLogger(__name__)

app = Flask(__name__)

pg_db_name = "simple_flask_app"
pg_db_port = "5432 "
pg_user = "rohit"
pg_host = "127.0.0.1"
pg_user_password = "rohit"

mysql_username = "rohit"
mysql_password = "rohit123#"
mysql_dbname = "test1"


@app.route('/')
def home():
    return render_template('front_page.html')


@app.route('/send_data', methods=['GET', 'POST'])
def get_form_data():
    if request.method == 'POST':
        column_name = request.form.get("column_name")
        if request.form.get("postgres"):
            try:
                conn = psycopg2.connect(database=pg_db_name, user=pg_user, password=pg_user_password, host=pg_host,
                                        port=pg_db_port)
                _logger.info("Database connection established successfully.")
                cursor = conn.cursor()
                cursor.execute("""SELECT EXISTS (
SELECT FROM pg_tables
WHERE tablename  = 'student');""")
                results = cursor.fetchall()[0]
                if not results[0]:
                    cursor.execute("""CREATE TABLE student
(ROLL INT     NOT NULL,
NAME           TEXT    NOT NULL,
AGE            INT     NOT NULL,
ADDRESS        CHAR(50),
CLASS        VARCHAR(20));""")
                    conn.commit()
                for i in range(1, 100):
                    name = ''.join(random.choice(ascii_letters) for _ in range(10))
                    age = i * 10
                    address = ''.join(random.choice(ascii_letters) for _ in range(20))
                    s_class = random.randint(1, 12)
                    value_tuple = (i, name, age, address, s_class)
                    cursor.execute("""INSERT INTO student (ROLL,NAME,AGE,ADDRESS,CLASS) VALUES {value};""".format(
                        value=value_tuple))
                    conn.commit()

                cursor.execute("SELECT %s from student;" % column_name)
                results = cursor.fetchall()
                cursor.close()
                to_return_data = jsonify(results)
                _logger.info(str(to_return_data))
                return to_return_data

            except Exception as e:
                _logger.error(str(e))

        elif request.form.get("sql"):
            try:
                engine = db.create_engine('mysql+pymysql://%s:%s@localhost/%s' % (mysql_username,
                                                                                  mysql_password,
                                                                                  mysql_dbname))
                connection = engine.connect()
                metadata = MetaData(engine)
                if not engine.dialect.has_table(engine.connect(), mysql_dbname):  # If table don't exist, Create.
                    metadata = MetaData(engine)
                    # Create a table with the appropriate Columns
                    students = Table(mysql_dbname, metadata,
                                     Column('Roll', Integer, nullable=False),
                                     Column('Name', String(50)), Column('Address', String(100)),
                                     Column('Age', Integer), Column('Class', String(10)))
                    # Implement the creation
                    metadata.create_all()
                else:
                    students = Table(mysql_dbname, metadata, autoload_with=engine)
                for i in range(1, 50):
                    name = ''.join(random.choice(ascii_letters) for _ in range(10))
                    age = i * 10
                    address = ''.join(random.choice(ascii_letters) for _ in range(20))
                    s_class = random.randint(1, 12)
                    ins = students.insert().values(Roll=i,
                                                   Name=name,
                                                   Address=address,
                                                   Age=age,
                                                   Class=s_class)

                    result = connection.execute(ins)
                t = text("SELECT %s from test"%column_name)
                result = connection.execute(t).fetchall()
                result = [tuple(x) for x in result]
                to_return_data = jsonify(result)
                # to_return_data = jsonify(result)
                _logger.info(str(to_return_data))
                return to_return_data

            except Exception as e:
                _logger.error(str(e))


if __name__ == '__main__':
    app.run(use_reloader=False, debug=True, threaded=False)
