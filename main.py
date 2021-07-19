from SQL import SQL
from flask import Flask, render_template, request, jsonify
import mysql.connector as connection
from Logger import Logger
app = Flask(__name__)

sql_ = ""


@app.route('/SQL/create_table', methods=['POST'])
def sql_create_table():
    body = request.json
    global sql_
    sql_ = SQL(body)
    try:
        return sql_.create_table(body)
    except Exception as e:
        return str(e)


@app.route('/SQL/Insert_Rows', methods=['POST'])
def sql_insert_rows():
    body = request.json
    try:
        return sql_.insert(body)
    except Exception as e:
        return str(e)


@app.route('/SQL/Update_Rows', methods=['POST'])
def sql_update_rows():
    body = request.json
    try:
        return sql_.update(body)
    except Exception as e:
        return str(e)


@app.route('/SQL/bulk_insertion', methods=['POST'])
def sql_bulk_insertion():
    body = request.json
    try:
        sql_ = SQL(body)
        return sql_.dump_file(body)
    except Exception as e:
        return str(e)


@app.route('/SQL/Delete_Rows', methods=['POST'])
def sql_delete_rows():
    body = request.json
    try:
        return sql_.delete_from_table(body)
    except Exception as e:
        return str(e)


@app.route('/SQL/Download_Data', methods=['POST'])
def sql_download_data():
    body = request.json
    try:
        return sql_.download_from_db(body)
    except Exception as e:
        return str(e)


if __name__ == '__main__':
    app.run()
