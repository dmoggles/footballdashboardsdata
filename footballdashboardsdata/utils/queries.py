from dbconnect.connector import Connection
from typing import List, Dict

def get_league_gender(fb_league_name: str) -> str:
    conn = Connection("M0neyMa$e")
    return conn.query(
        f"""SELECT gender FROM mclachbot_leagues WHERE league_name='{fb_league_name}'"""
    )["gender"][0]


def get_decorated_team_name_from_fb_name(fb_team_name: str, league_name: str) -> str:
    conn = Connection("M0neyMa$e")
    gender = get_league_gender(league_name)
    data = conn.query(
        f"""SELECT decorated_name FROM mclachbot_teams 

    WHERE team_name='{fb_team_name}' 
    AND gender = '{gender}'
    """
    )
    if len(data) == 0:
        return fb_team_name.replace("_", " ").title()
    else:
        return data["decorated_name"][0]


def get_decorated_league_name_from_fb_name(fb_league_name: str) -> str:
    conn = Connection("M0neyMa$e")
    return conn.query(
        f"""SELECT decorated_name FROM mclachbot_leagues WHERE league_name='{fb_league_name}'"""
    )["decorated_name"][0]


def get_multiple_decorated_league_names_from_fb_names(fb_league_names: List[str])->Dict[str, str]:
    conn = Connection("M0neyMa$e")
    return conn.query(
        f"""SELECT league_name, decorated_name FROM mclachbot_leagues WHERE league_name IN {tuple(fb_league_names)}"""
    ).set_index("league_name").to_dict()["decorated_name"]
