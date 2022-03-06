from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2
import json
from dotenv import dotenv_values

@app.route('/')
def index():
   print('Request for index page received')
   return render_template('index.html')


@app.route('/v1/health', methods=['GET'])
def dbs_je_best():
    auth = dotenv_values("/home/en_var.env")

    conn = psycopg2.connect(
        host="147.175.150.216",
        database="dota2",
        user=auth["DBUSER"],
        password=auth["DBPASS"])

    print('Request for /v1/health received')
    return "textik"


@app.route('/hello', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))


if __name__ == '__main__':
   app.run()