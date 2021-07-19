from flask import Flask, render_template,  jsonify
import mysql.connector as connection
from Logger import Logger

app = Flask(__name__)


class SQL:

    def __init__(self, request: dict):
        self.host = request['host']
        self.user = request['user']
        self.passwd = request['password']
        if "db" in request:
            self.db = request['db']
        self.logger = Logger("logfile.txt")
        self.logger.log("info", "SQL object created")

    def conn(self):
        try:
            if self.db == "":
                conn = connection.connect(host=self.host, user=self.user, passwd=self.passwd)
            else:
                conn = connection.connect(host=self.host, user=self.user, passwd=self.passwd, database=self.db)
            return conn
        except Exception as e:
            self.logger.log("error", f"connection error : {str(e)}")
            return str(e)

    def create_db(self, db_name):
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"create database {db_name}")
            self.db = db_name
            conn.close()
            self.logger.log("info", f"{db_name} DB created")
        except Exception as e:
            conn.close()
            self.logger.log("error", f"db not created error : {str(e)}")

    def create_table(self, table_details):
        for i in table_details['table'].values():
            col = ''
            for j in i['col']:
                col += j[0] + ','
            col = col.rstrip(',')
            try:
                conn = self.conn()
                cursor = conn.cursor()
                cursor.execute(f"CREATE TABLE {i['name']} ({col})")
                conn.close()
                self.logger.log("info", f"{i['name']} table created with columns: {col}")
                print("success")
                return "Success"
            except Exception as e:
                conn.close()
                self.logger.log("error", f"table {i['name']} not created error : {str(e)}")
                return "Fail"

    def insert(self, table_details):
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"INSERT INTO {table_details['name']} VALUES ({table_details['data']})")
            conn.commit()
            conn.close()
            return "Success"
        except Exception as e:
            conn.close()
            self.logger.log("error", f"insert error : {str(e)}")
            return "Fail"

    def update(self, update_details):
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"UPDATE {update_details['table_name']} SET {update_details['update_query']} "
                           f"WHERE {update_details['condition']}")
            conn.commit()
            conn.close()
            return "Table updated"
        except Exception as e:
            conn.close()
            self.logger.log("error", f"update error : {str(e)}")
            return "Error"

    def dump_file(self, bulk_details):
        try:
            f = open(bulk_details['f_name'], "r")
            f.readline()
            self.create_table(bulk_details)
            for line in f.readlines():
                data = "\'" + line[:-1].replace(";", "\',\'") + "\'"
                bulk_details['data'] = data
                self.insert(bulk_details)
            self.logger.log("info", f"{bulk_details['f_name']} file data dumped to table")
            return "Bulk insert completed"
        except Exception as e:
            self.logger.log("error", f"file dump error : {str(e)}")
            return "error"

    def delete_from_table(self, table_details):
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table_details['t_name']} WHERE {table_details['condition']}")
            conn.commit()
            conn.close()
            self.logger.log("info", f"data from {table_details['t_name']} table deleted")
            return "rows delete from table"
        except Exception as e:
            conn.close()
            self.logger.log("error", f"table not deleted error : {str(e)}")
            return "error"

    def download_from_db(self, table_details):
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"Select * from {table_details['t_name']}")
            with open(table_details['f_name'], 'a') as the_file:
                for result in cursor.fetchall():
                    the_file.writelines(str(result))
            conn.close()
            self.logger.log("info", f"{table_details['t_name']} table data downloaded")
            return "data downloaded in the file"
        except Exception as e:
            print(str(e))
            self.logger.log("error", f"table show error : {str(e)}")
            return "error"
