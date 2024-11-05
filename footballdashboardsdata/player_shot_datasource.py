from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection
from footmav.utils.whoscored_funcs import (
    minutes,
    in_rectangle,
    is_goal,
    col_has_qualifier,
    is_touch,
    in_attacking_box,
)
from footmav.data_definitions.whoscored.constants import EventType


class PlayerShotDatasource(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "player_shot_data"

    def _minutes_played(
        self, season: int, competition: str, team: str, player_id: int
    ) -> int:
        conn = Connection("M0neyMa$e")
        data = conn.query(
            f"""
            SELECT * FROM whoscored T1
            WHERE T1.season = {season}  
            AND T1.competition = '{competition}'     
            AND T1.team = '{team}'     
            AND T1.playerId = {player_id}
            
        """,
            lambda x: EventType(x),
        )
        num_minutes = minutes(data)["minutes"].sum()
        box_touches = (is_touch(data) & in_attacking_box(data, start=True)).sum()
        return num_minutes, box_touches

    def _get_player_id(
        self, season: int, competition: str, team: str, player: str
    ) -> int:
        conn = Connection("M0neyMa$e")
        data = conn.query(
            f"""
            SELECT DISTINCT(playerId) FROM whoscored WHERE season={season} AND
            competition='{competition}' AND team='{team}' AND player_name='{player}'
        """
        )
        return data["playerId"].iloc[0]

    def impl_get_data(self, season: int, competition: str, team: str, player: str):
        conn = Connection("M0neyMa$e")
        player_id = self._get_player_id(season, competition, team, player)
        gender = "w" if competition in ["WSL"] else "m"
        data = conn.query(
            f"""
        SELECT T1.*,T2.*, T3.*, T4.decorated_name AS decorated_team_name,
        T5.decorated_name as decorated_league_name FROM whoscored T1
        JOIN derived.whoscored_shot_data T2
        ON T1.id = T2.id
        JOIN derived.whoscored_extra_event_info T3
        ON T1.id = T3.id
        JOIN football_data.mclachbot_teams T4
        ON T1.team = T4.ws_team_name AND T4.gender='{gender}'
        JOIN football_data.mclachbot_leagues T5
        ON T1.competition = T5.ws_league_name
        WHERE T1.season = {season}  
        AND T1.competition = '{competition}'     
        AND T1.team = '{team}'     
        AND T1.playerId = {player_id}
        
        """,
            lambda x: EventType(x),
        )
        minutes, box_touches = self._minutes_played(
            season, competition, team, player_id
        )
        data["minutes"] = minutes
        data["box_touches"] = box_touches
        data["is_goal"] = is_goal(data)
        data["is_penalty"] = col_has_qualifier(data, "Penalty")
        data["right_foot"] = col_has_qualifier(data, "RightFoot")
        data["left_foot"] = col_has_qualifier(data, "LeftFoot")
        data["header"] = col_has_qualifier(data, qualifier_code=15)
        data = data.loc[:, ~data.columns.duplicated()].copy()
        return data[data["is_penalty"] == False]
