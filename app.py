from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psi
import json
from dotenv import dotenv_values

@app.route('/')
def index():
    return "<style>" \
               "button {" \
                    "background-color: #66B1FF;" \
                    "border: none;" \
                    "color: white;" \
                    "padding: 15px 32px;" \
                    "text-align: center;" \
                    "text-decoration: none;" \
                    "display: inline-block;" \
                    "font-size: 16px;" \
               "}" \
           "</style>" \
           "" \
           "<form action=\"https://fiit-dbs-xoross-app.azurewebsites.net/v1/health\"><button type=\"submit\">/v1/health</button></form><br>" \
           "<form action=\"https://fiit-dbs-xoross-app.azurewebsites.net/v2/patches\"><button type=\"submit\">/v2/patches</button></form><br>" \
           "<form action=\"https://fiit-dbs-xoross-app.azurewebsites.net/v2/players/14944/game_exp\"><button type=\"submit\">/v2/players/14944/game_exp</button></form><br>"
    #print('Request for index page received')
    #"<button type=\"button\">Click Me!</button>" #render_template('index.html')

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
"""


@app.route('/v2/<string:player_id>/game_exp', methods=['GET', 'POST'])
def v2_game_exp(player_id):

    return "/v2/player_id"


if __name__ == '__main__':
   app.run()