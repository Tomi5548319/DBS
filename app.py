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
        return get_linux_conn().cursor()
    return get_windows_conn().cursor()


@app.route('/v1/health/', methods=['GET'])
def v1_health():
    kurzor = connect_to_database()
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

    return json.dumps(moj_dic)


@app.route('/v2/patches/', methods=['GET'])
def v2_patches():
    kurzor = connect_to_database()
    kurzor.execute("SELECT patches.name as patch_version, "
                   "CAST( extract(epoch FROM patches.release_date) AS INT) as patch_start_date, "
                   "CAST( extract(epoch FROM patch2.release_date) AS INT) as patch_end_date, "
                   "all_matches.match_id, ROUND(all_matches.duration/60.0, 2) "
                   "FROM patches "
                   "LEFT JOIN patches as patch2 on patches.id = patch2.id - 1 "
                   "LEFT JOIN( "
                   "SELECT matches.id as match_id, duration, start_time "
                   "FROM matches "
                   ") as all_matches on all_matches.start_time > extract(epoch FROM patches.release_date) "
                   "and all_matches.start_time < COALESCE(extract(epoch FROM patch2.release_date), 9999999999) "
                   "ORDER by patches.id")

    vystup = {}
    vystup['patches'] = []

    for riadok in kurzor:
        act_patch = None

        for patch in vystup['patches']:
            if patch['patch_version'] == str(riadok[0]):
                act_patch = patch
                break

        if act_patch is not None:
            match = {}
            match['match_id'] = riadok[3]
            match['duration'] = float(riadok[4])

            act_patch['matches'].append(match)
        else:
            act_patch = {}
            act_patch['patch_version'] = str(riadok[0])
            act_patch['patch_start_date'] = riadok[1]
            act_patch['patch_end_date'] = riadok[2]
            act_patch['matches'] = []
            vystup['patches'].append(act_patch)

            if riadok[3] is not None and riadok[4] is not None:
                match = {}
                match['match_id'] = riadok[3]
                match['duration'] = float(riadok[4])
                act_patch['matches'].append(match)

    kurzor.close()

    return json.dumps(vystup)


@app.route('/v2/players/<string:player_id>/game_exp/', methods=['GET'])
def v2_game_exp(player_id):
    kurzor = connect_to_database()
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

    return json.dumps(hlavny_dic)

@app.route('/v2/players/<string:player_id>/game_objectives/', methods=['GET'])
def v2_game_objectives(player_id):
    kurzor = connect_to_database()

    hlavny_dic = {}
    hlavny_dic['id'] = int(player_id)

    kurzor.execute("SELECT p.id, p.nick AS player_nick, "
                   "mpd.match_id, heroes.localized_name, "
                   "COALESCE(game_objectives.subtype, 'NO_ACTION') "
                   "FROM players AS p "
                   "LEFT JOIN matches_players_details AS mpd ON mpd.player_id = p.id "
                   "LEFT JOIN heroes ON heroes.id = mpd.hero_id "
                   "LEFT JOIN game_objectives ON game_objectives.match_player_detail_id_1 = mpd.id "
                   "WHERE p.id = " + player_id +
                   " ORDER BY mpd.match_id")

    matches = []

    for row in kurzor:
        if not 'player_nick' in hlavny_dic.keys():
            hlavny_dic['player_nick'] = row[1]

        act_match = None
        for match in matches:
            if match['match_id'] == row[2]:
                act_match = match
                break

        if act_match is not None:
            act_action = None
            for action in act_match['actions']:
                if action['hero_action'] == row[4]:
                    act_action = action
                    break

            if act_action is not None:
                act_action['count'] += 1
            else:
                act_action = {}
                act_action['hero_action'] = row[4]
                act_action['count'] = 1

        else:
            act_match = {}
            act_match['match_id'] = row[2]
            act_match['hero_localized_name'] = row[3]
            matches.append(act_match)

            act_match['actions'] = []
            action = {}
            action['hero_action'] = row[4]
            action['count'] = 1
            act_match['actions'].append(action)

    hlavny_dic['matches'] = matches

    kurzor.close()

    return json.dumps(hlavny_dic)


if __name__ == '__main__':
   app.run()