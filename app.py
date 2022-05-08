from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for
app = Flask(__name__)

import psycopg2 as psi
import json
import os
import platform
from dotenv import dotenv_values
from flask_sqlalchemy import SQLAlchemy

#Base = declarative_base()
#metadata = Base.metadata

#alchemy_env = dotenv_values("/home/en_var.env")
#app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + alchemy_env['DBUSER'] + ':' + alchemy_env['DBPASS'] + '@147.175.150.216/dota2'
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://' + os.getenv('DBUSER') + ':' + os.getenv('DBPASS') + '@147.175.150.216/dota2'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

class Ability(db.Model):
    __tablename__ = 'abilities'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class AuthGroup(db.Model):
    __tablename__ = 'auth_group'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('auth_group_id_seq'::regclass)"))
    name = db.Column(db.String(150), nullable=False, unique=True)


class AuthUser(db.Model):
    __tablename__ = 'auth_user'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('auth_user_id_seq'::regclass)"))
    password = db.Column(db.String(128), nullable=False)
    last_login = db.Column(db.DateTime(True))
    is_superuser = db.Column(db.Boolean, nullable=False)
    username = db.Column(db.String(150), nullable=False, unique=True)
    first_name = db.Column(db.String(150), nullable=False)
    last_name = db.Column(db.String(150), nullable=False)
    email = db.Column(db.String(254), nullable=False)
    is_staff = db.Column(db.Boolean, nullable=False)
    is_active = db.Column(db.Boolean, nullable=False)
    date_joined = db.Column(db.DateTime(True), nullable=False)


class ClusterRegion(db.Model):
    __tablename__ = 'cluster_regions'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class DjangoContentType(db.Model):
    __tablename__ = 'django_content_type'
    __table_args__ = (
        db.UniqueConstraint('app_label', 'model'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('django_content_type_id_seq'::regclass)"))
    app_label = db.Column(db.String(100), nullable=False)
    model = db.Column(db.String(100), nullable=False)


class DjangoMigration(db.Model):
    __tablename__ = 'django_migrations'

    id = db.Column(db.BigInteger, primary_key=True, server_default=db.text("nextval('django_migrations_id_seq'::regclass)"))
    app = db.Column(db.String(255), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    applied = db.Column(db.DateTime(True), nullable=False)


class DjangoSession(db.Model):
    __tablename__ = 'django_session'

    session_key = db.Column(db.String(40), primary_key=True, index=True)
    session_data = db.Column(db.Text, nullable=False)
    expire_date = db.Column(db.DateTime(True), nullable=False, index=True)


class Hero(db.Model):
    __tablename__ = 'heroes'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    localized_name = db.Column(db.Text)


class Item(db.Model):
    __tablename__ = 'items'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)


class Patch(db.Model):
    __tablename__ = 'patches'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('patches_id_seq'::regclass)"))
    name = db.Column(db.Text, nullable=False)
    release_date = db.Column(db.DateTime, nullable=False)


class Player(db.Model):
    __tablename__ = 'players'

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.Text)
    nick = db.Column(db.Text)


class AuthPermission(db.Model):
    __tablename__ = 'auth_permission'
    __table_args__ = (
        db.UniqueConstraint('content_type_id', 'codename'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('auth_permission_id_seq'::regclass)"))
    name = db.Column(db.String(255), nullable=False)
    content_type_id = db.Column(db.ForeignKey('django_content_type.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    codename = db.Column(db.String(100), nullable=False)

    content_type = db.relationship('DjangoContentType')


class AuthUserGroup(db.Model):
    __tablename__ = 'auth_user_groups'
    __table_args__ = (
        db.UniqueConstraint('user_id', 'group_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=db.text("nextval('auth_user_groups_id_seq'::regclass)"))
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    group_id = db.Column(db.ForeignKey('auth_group.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = db.relationship('AuthGroup')
    user = db.relationship('AuthUser')


class DjangoAdminLog(db.Model):
    __tablename__ = 'django_admin_log'
    __table_args__ = (
        db.CheckConstraint('action_flag >= 0'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('django_admin_log_id_seq'::regclass)"))
    action_time = db.Column(db.DateTime(True), nullable=False)
    object_id = db.Column(db.Text)
    object_repr = db.Column(db.String(200), nullable=False)
    action_flag = db.Column(db.SmallInteger, nullable=False)
    change_message = db.Column(db.Text, nullable=False)
    content_type_id = db.Column(db.ForeignKey('django_content_type.id', deferrable=True, initially='DEFERRED'), index=True)
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    content_type = db.relationship('DjangoContentType')
    user = db.relationship('AuthUser')


class Match(db.Model):
    __tablename__ = 'matches'

    id = db.Column(db.Integer, primary_key=True)
    cluster_region_id = db.Column(db.ForeignKey('cluster_regions.id'))
    start_time = db.Column(db.Integer)
    duration = db.Column(db.Integer)
    tower_status_radiant = db.Column(db.Integer)
    tower_status_dire = db.Column(db.Integer)
    barracks_status_radiant = db.Column(db.Integer)
    barracks_status_dire = db.Column(db.Integer)
    first_blood_time = db.Column(db.Integer)
    game_mode = db.Column(db.Integer)
    radiant_win = db.Column(db.Boolean)
    negative_votes = db.Column(db.Integer)
    positive_votes = db.Column(db.Integer)

    cluster_region = db.relationship('ClusterRegion')


class PlayerRating(db.Model):
    __tablename__ = 'player_ratings'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('player_ratings_id_seq'::regclass)"))
    player_id = db.Column(db.ForeignKey('players.id'))
    total_wins = db.Column(db.Integer)
    total_matches = db.Column(db.Integer)
    trueskill_mu = db.Column(db.Numeric)
    trueskill_sigma = db.Column(db.Numeric)

    player = db.relationship('Player')


class AuthGroupPermission(db.Model):
    __tablename__ = 'auth_group_permissions'
    __table_args__ = (
        db.UniqueConstraint('group_id', 'permission_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=db.text("nextval('auth_group_permissions_id_seq'::regclass)"))
    group_id = db.Column(db.ForeignKey('auth_group.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    permission_id = db.Column(db.ForeignKey('auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = db.relationship('AuthGroup')
    permission = db.relationship('AuthPermission')


class AuthUserUserPermission(db.Model):
    __tablename__ = 'auth_user_user_permissions'
    __table_args__ = (
        db.UniqueConstraint('user_id', 'permission_id'),
    )

    id = db.Column(db.BigInteger, primary_key=True, server_default=db.text("nextval('auth_user_user_permissions_id_seq'::regclass)"))
    user_id = db.Column(db.ForeignKey('auth_user.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    permission_id = db.Column(db.ForeignKey('auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    permission = db.relationship('AuthPermission')
    user = db.relationship('AuthUser')


class MatchesPlayersDetail(db.Model):
    __tablename__ = 'matches_players_details'
    __table_args__ = (
        db.Index('idx_match_id_player_id', 'match_id', 'player_slot', 'id'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('matches_players_details_id_seq'::regclass)"))
    match_id = db.Column(db.ForeignKey('matches.id'))
    player_id = db.Column(db.ForeignKey('players.id'))
    hero_id = db.Column(db.ForeignKey('heroes.id'))
    player_slot = db.Column(db.Integer)
    gold = db.Column(db.Integer)
    gold_spent = db.Column(db.Integer)
    gold_per_min = db.Column(db.Integer)
    xp_per_min = db.Column(db.Integer)
    kills = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    assists = db.Column(db.Integer)
    denies = db.Column(db.Integer)
    last_hits = db.Column(db.Integer)
    stuns = db.Column(db.Integer)
    hero_damage = db.Column(db.Integer)
    hero_healing = db.Column(db.Integer)
    tower_damage = db.Column(db.Integer)
    item_id_1 = db.Column(db.ForeignKey('items.id'))
    item_id_2 = db.Column(db.ForeignKey('items.id'))
    item_id_3 = db.Column(db.ForeignKey('items.id'))
    item_id_4 = db.Column(db.ForeignKey('items.id'))
    item_id_5 = db.Column(db.ForeignKey('items.id'))
    item_id_6 = db.Column(db.ForeignKey('items.id'))
    level = db.Column(db.Integer)
    leaver_status = db.Column(db.Integer)
    xp_hero = db.Column(db.Integer)
    xp_creep = db.Column(db.Integer)
    xp_roshan = db.Column(db.Integer)
    xp_other = db.Column(db.Integer)
    gold_other = db.Column(db.Integer)
    gold_death = db.Column(db.Integer)
    gold_abandon = db.Column(db.Integer)
    gold_sell = db.Column(db.Integer)
    gold_destroying_structure = db.Column(db.Integer)
    gold_killing_heroes = db.Column(db.Integer)
    gold_killing_creeps = db.Column(db.Integer)
    gold_killing_roshan = db.Column(db.Integer)
    gold_killing_couriers = db.Column(db.Integer)

    hero = db.relationship('Hero')
    item = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_1 == Item.id')
    item1 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_2 == Item.id')
    item2 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_3 == Item.id')
    item3 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_4 == Item.id')
    item4 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_5 == Item.id')
    item5 = db.relationship('Item', primaryjoin='MatchesPlayersDetail.item_id_6 == Item.id')
    match = db.relationship('Match')
    player = db.relationship('Player')


class Teamfight(db.Model):
    __tablename__ = 'teamfights'
    __table_args__ = (
        db.Index('teamfights_match_id_start_teamfight_id_idx', 'match_id', 'start_teamfight', 'id'),
    )

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('teamfights_id_seq'::regclass)"))
    match_id = db.Column(db.ForeignKey('matches.id'))
    start_teamfight = db.Column(db.Integer)
    end_teamfight = db.Column(db.Integer)
    last_death = db.Column(db.Integer)
    deaths = db.Column(db.Integer)

    match = db.relationship('Match')


class AbilityUpgrade(db.Model):
    __tablename__ = 'ability_upgrades'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('ability_upgrades_id_seq'::regclass)"))
    ability_id = db.Column(db.ForeignKey('abilities.id'))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    level = db.Column(db.Integer)
    time = db.Column(db.Integer)

    ability = db.relationship('Ability')
    match_player_detail = db.relationship('MatchesPlayersDetail')


class Chat(db.Model):
    __tablename__ = 'chats'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('chats_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    message = db.Column(db.Text)
    time = db.Column(db.Integer)
    nick = db.Column(db.Text)

    match_player_detail = db.relationship('MatchesPlayersDetail')


class GameObjective(db.Model):
    __tablename__ = 'game_objectives'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('game_objectives_id_seq'::regclass)"))
    match_player_detail_id_1 = db.Column(db.ForeignKey('matches_players_details.id'))
    match_player_detail_id_2 = db.Column(db.ForeignKey('matches_players_details.id'))
    key = db.Column(db.Integer)
    subtype = db.Column(db.Text)
    team = db.Column(db.Integer)
    time = db.Column(db.Integer)
    value = db.Column(db.Integer)
    slot = db.Column(db.Integer)

    matches_players_detail = db.relationship('MatchesPlayersDetail', primaryjoin='GameObjective.match_player_detail_id_1 == MatchesPlayersDetail.id')
    matches_players_detail1 = db.relationship('MatchesPlayersDetail', primaryjoin='GameObjective.match_player_detail_id_2 == MatchesPlayersDetail.id')


class PlayerAction(db.Model):
    __tablename__ = 'player_actions'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('player_actions_id_seq'::regclass)"))
    unit_order_none = db.Column(db.Integer)
    unit_order_move_to_position = db.Column(db.Integer)
    unit_order_move_to_target = db.Column(db.Integer)
    unit_order_attack_move = db.Column(db.Integer)
    unit_order_attack_target = db.Column(db.Integer)
    unit_order_cast_position = db.Column(db.Integer)
    unit_order_cast_target = db.Column(db.Integer)
    unit_order_cast_target_tree = db.Column(db.Integer)
    unit_order_cast_no_target = db.Column(db.Integer)
    unit_order_cast_toggle = db.Column(db.Integer)
    unit_order_hold_position = db.Column(db.Integer)
    unit_order_train_ability = db.Column(db.Integer)
    unit_order_drop_item = db.Column(db.Integer)
    unit_order_give_item = db.Column(db.Integer)
    unit_order_pickup_item = db.Column(db.Integer)
    unit_order_pickup_rune = db.Column(db.Integer)
    unit_order_purchase_item = db.Column(db.Integer)
    unit_order_sell_item = db.Column(db.Integer)
    unit_order_disassemble_item = db.Column(db.Integer)
    unit_order_move_item = db.Column(db.Integer)
    unit_order_cast_toggle_auto = db.Column(db.Integer)
    unit_order_stop = db.Column(db.Integer)
    unit_order_buyback = db.Column(db.Integer)
    unit_order_glyph = db.Column(db.Integer)
    unit_order_eject_item_from_stash = db.Column(db.Integer)
    unit_order_cast_rune = db.Column(db.Integer)
    unit_order_ping_ability = db.Column(db.Integer)
    unit_order_move_to_direction = db.Column(db.Integer)
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))

    match_player_detail = db.relationship('MatchesPlayersDetail')


class PlayerTime(db.Model):
    __tablename__ = 'player_times'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('player_times_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    time = db.Column(db.Integer)
    gold = db.Column(db.Integer)
    lh = db.Column(db.Integer)
    xp = db.Column(db.Integer)

    match_player_detail = db.relationship('MatchesPlayersDetail')


class PurchaseLog(db.Model):
    __tablename__ = 'purchase_logs'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('purchase_logs_id_seq'::regclass)"))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    item_id = db.Column(db.ForeignKey('items.id'))
    time = db.Column(db.Integer)

    item = db.relationship('Item')
    match_player_detail = db.relationship('MatchesPlayersDetail')


class TeamfightsPlayer(db.Model):
    __tablename__ = 'teamfights_players'

    id = db.Column(db.Integer, primary_key=True, server_default=db.text("nextval('teamfights_players_id_seq'::regclass)"))
    teamfight_id = db.Column(db.ForeignKey('teamfights.id'))
    match_player_detail_id = db.Column(db.ForeignKey('matches_players_details.id'))
    buyback = db.Column(db.Integer)
    damage = db.Column(db.Integer)
    deaths = db.Column(db.Integer)
    gold_delta = db.Column(db.Integer)
    xp_start = db.Column(db.Integer)
    xp_end = db.Column(db.Integer)

    match_player_detail = db.relationship('MatchesPlayersDetail')
    teamfight = db.relationship('Teamfight')



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
"<button onclick=\"changeURL('v2/players/14944/abilities/');\">/v2/players/14944/abilities/</button></form><br><br>" \
"<button onclick=\"changeURL('v3/matches/21421/top_purchases/');\">/v3/matches/21421/top_purchases/</button></form><br><br>" \
"<button onclick=\"changeURL('v3/abilities/5004/usage/');\">/v3/abilities/5004/usage/</button></form><br><br>" \
"<button onclick=\"changeURL('v3/statistics/tower_kills/');\">/v3/statistics/tower_kills/</button></form><br><br>" \
"<button onclick=\"changeURL('v4/players/14944/game_objectives/');\">/v4/players/14944/game_objectives/</button></form><br><br>"


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

    kurzor.execute("SELECT p.id, COALESCE(p.nick, 'unknown') AS player_nick, "
                   "mpd.match_id, heroes.localized_name, "
                   "COALESCE(game_objectives.subtype, 'NO_ACTION') "
                   "FROM players AS p "
                   "LEFT JOIN matches_players_details AS mpd ON mpd.player_id = p.id "
                   "LEFT JOIN heroes ON heroes.id = mpd.hero_id "
                   "LEFT JOIN game_objectives ON game_objectives.match_player_detail_id_1 = mpd.id "
                   "WHERE p.id = " + player_id +
                   " ORDER BY mpd.match_id, subtype")

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
                act_match['actions'].append(act_action)

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


@app.route('/v2/players/<string:player_id>/abilities/', methods=['GET'])
def v2_abilities(player_id):
    kurzor = connect_to_database()

    hlavny_dic = {}
    hlavny_dic['id'] = int(player_id)

    kurzor.execute("SELECT p.id, COALESCE(p.nick, 'unknown') AS player_nick, "
                   "mpd.match_id, heroes.localized_name, "
                   "abilities.name, au.level "
                   "FROM players AS p "
                   "LEFT JOIN matches_players_details AS mpd ON mpd.player_id = p.id "
                   "LEFT JOIN heroes ON heroes.id = mpd.hero_id "
                   "LEFT JOIN ability_upgrades AS au ON au.match_player_detail_id = mpd.id "
                   "LEFT JOIN abilities ON abilities.id = au.ability_id "
                   "WHERE p.id = " + player_id +
                   " ORDER BY mpd.match_id, abilities.name, au.level ")

    matches = []

    for row in kurzor:
        if not 'player_nick' in hlavny_dic.keys():
            hlavny_dic['player_nick'] = row[1]

        act_match = None
        for match in matches:
            if match['match_id'] == row[2]:
                act_match = match
                break

        # Aktualny match uz pozname
        if act_match is not None:
            act_ability = None
            for ability in act_match['abilities']:
                if ability['ability_name'] == row[4]:
                    act_ability = ability
                    break

            # Aktualna ability uz bola ulozena
            if act_ability is not None:
                act_ability['count'] += 1
                act_ability['upgrade_level'] = row[5]

            # Aktualna ability je nova pre aktualny match
            else:
                act_ability = {}
                act_ability['ability_name'] = row[4]
                act_ability['count'] = 1
                act_ability['upgrade_level'] = row[5]
                act_match['abilities'].append(act_ability)

        # Aktualny match este nepozname
        else:
            act_match = {}
            act_match['match_id'] = row[2]
            act_match['hero_localized_name'] = row[3]
            matches.append(act_match)

            act_match['abilities'] = []
            ability = {}
            ability['ability_name'] = row[4]
            ability['count'] = 1
            ability['upgrade_level'] = row[5]
            act_match['abilities'].append(ability)

    hlavny_dic['matches'] = matches

    kurzor.close()

    return json.dumps(hlavny_dic)


@app.route('/v3/matches/<string:match_id>/top_purchases/', methods=['GET'])
def v3_matches_topPurchases(match_id):
    kurzor = connect_to_database()

    hlavny_dic = {}
    hlavny_dic['id'] = int(match_id)

    kurzor.execute("SELECT * FROM (SELECT localized_name, hero_id, item_id, items.name,  "
                   "COUNT (items.id) AS counter, row_number() over (partition by localized_name ORDER BY count(items.id) DESC, items.name ASC) "
                   "FROM matches_players_details "
                   "JOIN heroes ON hero_id = heroes.id "
                   "JOIN purchase_logs ON match_player_detail_id = matches_players_details.id "
                   "JOIN items ON item_id = items.id "
                   "JOIN matches ON matches_players_details.match_id = matches.id "
                   "WHERE match_id = " + match_id + " AND (player_slot < 100 AND radiant_win = 'True' OR player_slot >= 100 AND radiant_win = 'False') "
                   "GROUP BY hero_id, localized_name, item_id, items.name "
                   "ORDER BY hero_id, counter DESC, items.name)AS vypis "
                   "WHERE row_number < 6")

    heroes = []

    for row in kurzor:

        act_hero = None
        for hero in heroes:
            if hero['id'] == row[1]:
                act_hero = hero
                break

        if act_hero is None:

            act_hero = {}
            act_hero['id'] = row[1]
            act_hero['name'] = row[0]

            purchases = []

            purchase = {}
            purchase['id'] = row[2]
            purchase['name'] = row[3]
            purchase['count'] = row[4]
            purchases.append(purchase)

            act_hero['top_purchases'] = purchases

            heroes.append(act_hero)

        else:
            purchases = act_hero['top_purchases']

            purchase = {}
            purchase['id'] = row[2]
            purchase['name'] = row[3]
            purchase['count'] = row[4]
            purchases.append(purchase)

    hlavny_dic['heroes'] = heroes

    kurzor.close()

    return json.dumps(hlavny_dic)


@app.route('/v3/abilities/<string:ability_id>/usage/', methods=['GET'])
def v3_abilities_usage(ability_id):
    kurzor = connect_to_database()

    hlavny_dic = {}
    hlavny_dic['id'] = int(ability_id)

    kurzor.execute("SELECT * "
                   "FROM ( "
                   "SELECT *, rank() OVER (PARTITION BY winner, hero_id ORDER BY pocet DESC) "
                   "FROM ( "
                   "SELECT hero_id, localized_name, query2.name, rozsah, winner, count(*) AS pocet "
                   "FROM ( "
                   "SELECT hero_id, ability_id, query1.name, localized_name, winner, "
                   "CASE WHEN bucket > 0 AND bucket < 10 THEN '0-9' "
                   "WHEN bucket >= 10 AND bucket < 20 THEN '10-19' "
                   "WHEN bucket >= 20 AND bucket < 30 THEN '20-29' "
                   "WHEN bucket >= 30 AND bucket < 40 THEN '30-39' "
                   "WHEN bucket >= 40 AND bucket < 50 THEN '40-49' "
                   "WHEN bucket >= 50 AND bucket < 60 THEN '50-59' "
                   "WHEN bucket >= 60 AND bucket < 70 THEN '60-69' "
                   "WHEN bucket >= 70 AND bucket < 80 THEN '70-79' "
                   "WHEN bucket >= 80 AND bucket < 90 THEN '80-89' "
                   "WHEN bucket >= 90 AND bucket < 100 THEN '90-99' "
                   "WHEN bucket >= 100 THEN '100-109' END AS rozsah "
                   "FROM "
                   "( "
                   "SELECT hero_id, match_id, ability_id, abilities.name, localized_name, "
                   "CASE WHEN player_slot <= 5 AND radiant_win = 'True' THEN 'True' WHEN player_slot >= 100 AND radiant_win = 'False' THEN 'True' ELSE 'False' END AS winner, "
                   "duration, ability_upgrades.time, CAST( ability_upgrades.time AS FLOAT)/CAST( duration AS FLOAT) * 100 AS bucket "
                   "FROM matches_players_details "
                   "JOIN heroes ON hero_id = heroes.id "
                   "JOIN matches ON match_id = matches.id "
                   "JOIN ability_upgrades ON match_player_detail_id = matches_players_details.id "
                   "JOIN abilities ON ability_id = abilities.id "
                   "WHERE ability_id = " + ability_id +
                   " ORDER BY localized_name, bucket ASC "
                   ") AS query1 "
                   ") AS query2 "
                   "GROUP BY hero_id, rozsah, ability_id, query2.name, query2.localized_name, winner "
                   "ORDER BY pocet DESC, hero_id ASC "
                   ") AS query3 "
                   ") AS query4 "
                   "WHERE rank = 1")

    heroes = []

    for row in kurzor:
        hlavny_dic['name'] = row[2]

        act_hero = None

        for hero in heroes:
            if hero['id'] == row[0]:
                act_hero = hero
                break

        if act_hero is None:
            act_hero = {}
            act_hero['id'] = row[0]
            act_hero['name'] = row[1]

            if row[4] == 'True':
                act_hero['usage_winners'] = {}
                act_hero['usage_winners']['bucket'] = row[3]
                act_hero['usage_winners']['count'] = row[5]
            else:
                act_hero['usage_loosers'] = {}
                act_hero['usage_loosers']['bucket'] = row[3]
                act_hero['usage_loosers']['count'] = row[5]

            heroes.append(act_hero)

        else:
            if row[4] == 'True':
                act_hero['usage_winners'] = {}
                act_hero['usage_winners']['bucket'] = row[3]
                act_hero['usage_winners']['count'] = row[5]
            else:
                act_hero['usage_loosers'] = {}
                act_hero['usage_loosers']['bucket'] = row[3]
                act_hero['usage_loosers']['count'] = row[5]


    hlavny_dic['heroes'] = heroes

    kurzor.close()

    return json.dumps(hlavny_dic)


@app.route('/v3/statistics/tower_kills/', methods=['GET'])
def v3_statistics():
    kurzor = connect_to_database()

    hlavny_dic = {}

    kurzor.execute("SELECT * "
                   "FROM ( "
                   "SELECT DISTINCT ON (localized_name) localized_name, COUNT(*), id AS hero_id "
                   "FROM "
                   "( "
                   "SELECT hero_id AS id, match_id, localized_name, hero_id, time, subtype, row_number() OVER (PARTITION BY match_id ORDER BY time) - row_number() OVER (PARTITION BY match_id, hero_id ORDER BY time) AS riadok "
                   "FROM game_objectives "
                   "JOIN matches_players_details ON match_player_detail_id_1 = matches_players_details.id "
                   "JOIN heroes ON heroes.id = hero_id "
                   "WHERE subtype = 'CHAT_MESSAGE_TOWER_KILL' "
                   "GROUP BY match_id, localized_name, hero_id, time, subtype "
                   "ORDER BY match_id ASC, time ASC "
                   ") AS query1 "
                   "GROUP BY id, match_id, localized_name, riadok "
                   "ORDER BY localized_name, count DESC "
                   ") AS query2 "
                   "ORDER BY count DESC, localized_name ASC")

    heroes = []

    for row in kurzor:
        hero = {}
        hero['id'] = row[2]
        hero['name'] = row[0]
        hero['tower_kills'] = row[1]
        heroes.append(hero)

    hlavny_dic['heroes'] = heroes

    kurzor.close()

    return json.dumps(hlavny_dic)


@app.route('/v4/players/<string:player_id>/game_objectives/', methods=['GET'])
def v4_game_objectives(player_id):
    kurzor = connect_to_database()

    hlavny_dic = {}
    hlavny_dic['id'] = int(player_id)

    result = Player.query.join(MatchesPlayersDetail, MatchesPlayersDetail.player_id == Player.id, isouter = True)\
        .join(Hero, Hero.id == MatchesPlayersDetail.hero_id, isouter = True)\
        .join(GameObjective, GameObjective.match_player_detail_id_1 == MatchesPlayersDetail.id, isouter = True)\
        .filter(Player.id == player_id).order_by(MatchesPlayersDetail.match_id, GameObjective.subtype).\
        add_columns(Player.id, db.func.coalesce(Player.nick, 'unknown').label('nick'), MatchesPlayersDetail.match_id, Hero.localized_name, db.func.coalesce(GameObjective.subtype, 'NO_ACTION').label('subtype')).all()

    matches = []

    for row in result:
        if not 'player_nick' in hlavny_dic.keys():
            hlavny_dic['player_nick'] = row.nick

        act_match = None
        for match in matches:
            if match['match_id'] == row.match_id:
                act_match = match
                break

        if act_match is not None:
            act_action = None
            for action in act_match['actions']:
                if action['hero_action'] == row.subtype:
                    act_action = action
                    break

            if act_action is not None:
                act_action['count'] += 1
            else:
                act_action = {}
                act_action['hero_action'] = row.subtype
                act_action['count'] = 1
                act_match['actions'].append(act_action)

        else:
            act_match = {}
            act_match['match_id'] = row.match_id
            act_match['hero_localized_name'] = row.localized_name
            matches.append(act_match)

            act_match['actions'] = []
            action = {}
            action['hero_action'] = row.subtype
            action['count'] = 1
            act_match['actions'].append(action)

    hlavny_dic['matches'] = matches

    return json.dumps(hlavny_dic)


if __name__ == '__main__':
   app.run()