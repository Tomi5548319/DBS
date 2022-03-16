from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psi
import json
import os
import platform
from dotenv import dotenv_values


def get_linux_conn():
    auth = dotenv_values("/home/en_var.env")

    return psi.connect(
        host="147.175.150.216",
        database="dota2",
        user=auth["DBUSER"],
        password=auth["DBPASS"])


def get_windows_conn():
    return psi.connect(
        host="147.175.150.216",
        database="dota2",
        user=os.getenv("DBUSER"),
        password=os.getenv("DBPASS"))


def connect_to_database():
    if platform.system() == "Linux":
        return get_linux_conn()
    return get_windows_conn()


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
            "<script>" \
                "function changeURL(extension) {" \
                "window.location.href = window.location.href + extension" \
           "}" \
           "</script>" \
           "<button onclick=\"changeURL('v1/health/');\">/v1/health/</button><br><br>" \
           "<button onclick=\"changeURL('v2/patches/');\">/v2/patches/</button></form><br><br>" \
           "<button onclick=\"changeURL('v2/players/14944/game_exp/');\">/v2/players/14944/game_exp/</button></form><br><br>"

        #print('Request for index page received')
    #"<button type=\"button\">Click Me!</button>" #render_template('index.html')


@app.route('/v1/health/', methods=['GET'])
def dbs_je_best():
    conn = connect_to_database()

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

    kurzor.close()
    conn.close()

    return json.dumps(moj_dic)


@app.route('/v2/players/<string:player_id>/game_exp/', methods=['GET', 'POST'])
def v2_game_exp(player_id):
    conn = connect_to_database()

    kurzor = conn.cursor()
    kurzor.execute("SELECT vysledok.pid AS id, vysledok.player_nick, vysledok.match_id, vysledok.h_name AS hero_localized_name, "
                   "vysledok.min AS match_duration_minutes, vysledok.experiences_gained, "
                   "vysledok.level_gained, "
                   "CASE WHEN side_played = 'radiant' AND vysledok.radiant_win = 'true' OR "
                   "side_played = 'dire' AND vysledok.radiant_win = 'false' "
                   "THEN 'true' ELSE 'false' END AS winner "
                   "FROM ("
                   "SELECT players.id AS pid, COALESCE(nick, 'unknown') AS player_nick, heroes.localized_name AS h_name, "
                   "matches.id AS match_id, matches.duration, ROUND(matches.duration/60.0, 2) AS min, "
                   "mpd.level AS level_gained, "
                   "COALESCE(mpd.xp_hero, 0) + COALESCE(mpd.xp_creep, 0) + "
                   "COALESCE(mpd.xp_other, 0) + COALESCE(mpd.xp_roshan, 0) AS experiences_gained, "
                   "mpd.player_slot, "
                   "CASE WHEN mpd.player_slot < 5 THEN 'radiant' ELSE 'dire' END AS side_played, "
                   "matches.radiant_win "
                   "FROM matches_players_details AS mpd "
                   "JOIN players ON players.id = mpd.player_id "
                   "JOIN heroes ON heroes.id = mpd.hero_id "
                   "JOIN matches ON matches.id = mpd.match_id "
                   "WHERE players.id = " + player_id +
                    " ORDER BY matches.id"
                   ") AS vysledok")

    hlavny_dic = {}
    for row in kurzor:
        hlavny_dic['id'] = row[0]
        break

    for row in kurzor:
        hlavny_dic['player_nick'] = row[1]
        break

    matches = []

    for row in kurzor:
        match_dic = {}
        match_dic['match_id'] = row[2]
        match_dic['hero_localized_name'] = row[3]
        match_dic['match_duration_minutes'] = float(row[4])
        match_dic['experiences_gained'] = row[5]
        match_dic['level_gained'] = row[6]
        match_dic['winner'] = ("" + row[7]) in ['true', '1', 't', 'y', 'yes']

        matches.append(match_dic)

    hlavny_dic['matches'] = matches


    #response_version = kurzor.fetchone()

    kurzor.close()
    conn.close()

    return json.dumps(hlavny_dic)
    #navrat  #"/v2/{player_id}/game_exp, {player_id} = " + player_id + "; <br>version: "
           # + type(response_version) # Toto to crashne


if __name__ == '__main__':
   app.run()