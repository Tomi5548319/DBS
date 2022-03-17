from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psi
import json
import os
import platform
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
"<script>" \
    "function changeURL(extension) {" \
        "window.location.href = window.location.href + extension" \
    "}" \
"</script>" \
"<button onclick=\"changeURL('v1/health/');\">/v1/health/</button><br><br>" \
"<button onclick=\"changeURL('v2/patches/');\">/v2/patches/</button></form><br><br>" \
"<button onclick=\"changeURL('v2/players/14944/game_exp/');\">/v2/players/14944/game_exp/</button></form><br><br>" \
"<button onclick=\"changeURL('v2/players/14944/game_objectives/');\">/v2/players/14944/game_objectives/</button></form><br><br>" \
"<button onclick=\"changeURL('v2/players/14944/abilities/');\">/v2/players/14944/abilities/</button></form><br><br>"


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


@app.route('/v1/health/', methods=['GET'])
def v1_health():
    conn = connect_to_database()

    kurzor = conn.cursor()
    kurzor.execute("SELECT VERSION();")
    response_version = kurzor.fetchone()[0]

    kurzor.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")
    response_db_size = kurzor.fetchone()[0]

    moj_dic = {}
    moj_vnoreny_dic = {}

    moj_vnoreny_dic["version"] = response_version
    moj_vnoreny_dic["dota2_db_size"] = response_db_size

    moj_dic['pgsql'] = moj_vnoreny_dic

    kurzor.close()
    conn.close()

    return json.dumps(moj_dic)


@app.route('/v2/patches/', methods=['GET'])
def v2_patches():
    conn = connect_to_database()

    kurzor = conn.cursor()
    kurzor.execute("SELECT name, release_date FROM patches ORDER BY release_date")

    patches = []

    for row in kurzor:
        patch = {}
        patch["patch_version"] = row[0]
        patch["patch_start_date"] = int((row[1] - datetime(1970, 1, 1)).total_seconds())
        patch["patch_end_date"] = None
        patch["matches"] = []

        patches.append(patch)

    for patch in range(len(patches)-1):
        patches[patch]["patch_end_date"] = patches[patch+1]["patch_start_date"]

    kurzor.execute("SELECT id, start_time, ROUND(matches.duration/60.0, 2) FROM matches")

    for row in kurzor:
        for patch in patches:
            if patch["patch_start_date"] < row[1] and (patch["patch_end_date"] is None or patch["patch_end_date"] > row[1]):
                matchdata = {}
                matchdata["match_id"] = row[0]
                matchdata["duration"] = float(row[2])

                patch["matches"].append(matchdata)
                continue

    hlavny_dic = {}
    hlavny_dic["patches"] = patches
    return json.dumps(hlavny_dic)


@app.route('/v2/players/<string:player_id>/game_exp/', methods=['GET'])
def v2_game_exp(player_id):
    conn = connect_to_database()

    kurzor = conn.cursor()
    kurzor.execute("SELECT COALESCE(nick, 'unknown') "
                   "FROM players "
                   "WHERE id = " + player_id)

    hlavny_dic = {}
    hlavny_dic['id'] = int(player_id)
    hlavny_dic['player_nick'] = kurzor.fetchone()[0]

    kurzor.execute("SELECT vysledok.match_id, vysledok.h_name AS hero_localized_name, "
                   "vysledok.min AS match_duration_minutes, vysledok.experiences_gained, "
                   "vysledok.level_gained, "
                   "CASE WHEN side_played = 'radiant' AND vysledok.radiant_win = 'true' OR "
                   "side_played = 'dire' AND vysledok.radiant_win = 'false' "
                   "THEN true ELSE false END AS winner "
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

    matches = []

    for row in kurzor:
        match_dic = {}
        match_dic['match_id'] = row[0]
        match_dic['hero_localized_name'] = row[1]
        match_dic['match_duration_minutes'] = float(row[2])
        match_dic['experiences_gained'] = row[3]
        match_dic['level_gained'] = row[4]
        match_dic['winner'] = row[5]

        matches.append(match_dic)

    hlavny_dic['matches'] = matches

    kurzor.close()
    conn.close()

    return json.dumps(hlavny_dic)

@app.route('/v2/players/<string:player_id>/game_objectives/', methods=['GET'])
def v2_game_objectives(player_id):
    conn = connect_to_database()

    kurzor = conn.cursor()
    kurzor.execute("SELECT COALESCE(nick, 'unknown') "
                   "FROM players "
                   "WHERE id = " + player_id)

    hlavny_dic = {}
    hlavny_dic['id'] = int(player_id)
    hlavny_dic['player_nick'] = kurzor.fetchone()[0]

    kurzor.execute("SELECT vysledok.match_id, vysledok.h_name AS hero_localized_name, "
                   "vysledok.min AS match_duration_minutes, vysledok.experiences_gained, "
                   "vysledok.level_gained, "
                   "CASE WHEN side_played = 'radiant' AND vysledok.radiant_win = 'true' OR "
                   "side_played = 'dire' AND vysledok.radiant_win = 'false' "
                   "THEN true ELSE false END AS winner "
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

    matches = []

    for row in kurzor:
        match_dic = {}
        match_dic['match_id'] = row[0]
        match_dic['hero_localized_name'] = row[1]

        actions = []
        match_dic['actions'] = actions

        matches.append(match_dic)

    kurzor.execute("SELECT COALESCE(match_player_detail_id_1, -1), COALESCE(match_player_detail_id_2, -1), subtype "
                   "FROM game_objectives AS game_obj")
    """
    for row in kurzor:
        if...
    """

    kurzor.close()
    conn.close()

    hlavny_dic['matches'] = matches
    return json.dumps(hlavny_dic)


if __name__ == '__main__':
   app.run()