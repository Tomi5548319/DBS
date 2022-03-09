from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psi
import json
from dotenv import dotenv_values


@app.route('/')
def index():
   #print('Request for index page received')
   return "Homepage" #render_template('index.html')


"""
@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       return render_template('hello.html', name = name)
   else:
       return redirect(url_for('index'))
"""


@app.route('/v1/health', methods=['GET'])
def dbs_je_best():
    auth = dotenv_values("/home/en_var.env")

    conn = psi.connect(
        host="147.175.150.216",
        database="dota2",
        user=auth["DBUSER"],
        password=auth["DBPASS"])

    kurzor = conn.cursor()
    kurzor.execute("SELECT VERSION();")
    response_version = kurzor.fetchone()

    kurzor.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")
    response_db_size = kurzor.fetchone()

    moj_dic = {}
    moj_vnoreny_dic = {}

    moj_vnoreny_dic["version"] = response_version[0]
    moj_vnoreny_dic["dota2_db_size"] = response_db_size[0]

    moj_dic['pgsql'] = moj_vnoreny_dic

    return json.dumps(moj_dic)

@app.route('/v2/patches', methods=['GET'])
def dbs_je_best():

    return "TODO"


if __name__ == '__main__':
   app.run()