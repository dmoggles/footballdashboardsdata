from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection
from footmav.utils import whoscored_funcs as WF
from footmav.data_definitions.whoscored.constants import EventType


class ShotDataSource(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "ShotData"

    def impl_get_data(self, match_id: str):
        conn = Connection("M0neyMa$e")
        query = f"""
            SELECT fbref_shots.*, 
            T1.decorated_name as squad_decorated_name,
            T2.decorated_name as league_decorated_name,
            T3.home
            FROM fbref_shots
            LEFT JOIN mclachbot_teams T1 ON 
            	fbref_shots.squad = T1.team_name
                AND fbref_shots.gender = T1.gender
            LEFT JOIN mclachbot_leagues T2 ON fbref_shots.comp = T2.league_name
            LEFT JOIN 
            (SELECT DISTINCT(squad) as squad, home FROM fbref WHERE match_id='{match_id}') T3
            ON fbref_shots.squad = T3.squad
            WHERE fbref_shots.match_id = '{match_id}'


            

        """

        data = conn.query(query)
        data["is_open_play"] = 1
        prev_i = None
        for i, row in data.iterrows():
            if row["notes"] == "penalty":
                data.loc[i, "is_open_play"] = 0
            if row["sca_1_event"] in ("Pass (Dead)", "Fouled"):
                data.loc[i, "is_open_play"] = 0
            if prev_i:
                prev_row = data.loc[prev_i]
                if (
                    prev_row["is_open_play"] == 0
                    and prev_row["minute"] == row["minute"]
                    and prev_row["squad"] == row["squad"]
                ):
                    data.loc[i, "is_open_play"] = 0
            prev_i = i

        return data


class ShotDataSourceEvents(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "ShotDataEvents"

    def impl_get_data(self, match_id: str):
        conn = Connection("M0neyMa$e")
        query = f"""
            SELECT * FROM football_data.whoscored T1
            INNER JOIN derived.whoscored_shot_data T2
            ON T1.id = T2.id
            WHERE T1.matchId = '{match_id}'
            AND event_type IN (13,14,15,16)


            

        """
        data = conn.wsquery(query)
        data["is_open_play"] = (
            (~WF.col_has_qualifier(data, qualifier_code=24))
            & (~WF.col_has_qualifier(data, qualifier_code=25))
            & (~WF.col_has_qualifier(data, qualifier_code=26))
            & (~WF.col_has_qualifier(data, display_name="Penalty"))
        ).astype(int)
        data["outcome"] = data["event_type"].map(
            {
                EventType.MissedShots: "Miss",
                EventType.ShotOnPost: "Post",
                EventType.SavedShot: "Saved",
                EventType.Goal: "Goal",
            }
        )
        data = data.rename(columns={"team": "squad", "is_home_team": "home"})
        return data[["minute", "squad", "outcome", "home", "xg", "is_open_play"]]
