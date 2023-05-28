import pandas as pd
from abc import ABC, abstractmethod
from footballdashboardsdata.utils.subclassing import get_all_subclasses
from dbconnect.connector import Connection


def attach_positional_data(conn, data):
        position_df = conn.query(
            "SELECT * FROM football_data.whoscored_positions"
        )
        position_df["formation_name"] = position_df["formation_name"].apply(
            lambda x: x.replace("-", "")
        )
        data = pd.merge(
            data,
            position_df,
            left_on=["formation", "position"],
            right_on=["formation_name", "position"],
            suffixes=("", "_"),
            how="left",
        )
        return data

def get_dataframe_for_match(match_id: int, conn: Connection):
    query1 = f"""
    SELECT W.*,
    E.shirt_number, E.formation, E.position, E.pass_receiver, E.pass_receiver_shirt_number, E.pass_receiver_position,
    G.game_state,
    P.passtypes,
    X.value as xT,
    XGT.xg AS xG,
    T1.decorated_name as decorated_team_name,
    T2.decorated_name as decorated_opponent_name,
    L1.decorated_name as decorated_league_name,
    MET.home_score, MET.away_score
    FROM whoscored W 
    LEFT JOIN derived.whoscored_extra_event_info E
    ON W.id=E.id
    LEFT JOIN derived.whoscored_game_state G
    ON W.id=G.id
    LEFT JOIN derived.whoscored_pass_types P
    ON W.id=P.id
    LEFT JOIN derived.whoscored_xthreat X
    on W.id=X.id
    LEFT JOIN derived.whoscored_shot_data XGT
    on W.id=XGT.id
    LEFT JOIN mclachbot_teams T1
    ON W.team = T1.ws_team_name
    LEFT JOIN mclachbot_teams T2
    ON W.opponent = T2.ws_team_name
    LEFT JOIN mclachbot_leagues L1
    ON W.competition = L1.ws_league_name
    LEFT JOIN whoscored_meta MET
    ON W.matchId=MET.matchId
    WHERE W.matchId={match_id} AND T1.gender='m'
    """

    query2 = f"""
    SELECT W.eventId,
           W.minute,
           W.second,
           W.x,
           W.y,
           W.qualifiers,
           W.period,
           W.event_type,
           W.outcomeType,
           W.endX,
           W.endY,
           W.matchId,
           W.season,
           W.competition,
           W.player_name,
           W.match_seconds,
           W.team,opponent,
           W.is_home_team,
           W.sub_id, 
           W.carryId,
           T1.decorated_name as decorated_team_name,
           T2.decorated_name as decorated_opponent_name,
           L1.decorated_name as decorated_league_name
           FROM derived.whoscored_implied_carries_v2 W
           LEFT JOIN mclachbot_teams T1
           ON W.team = T1.ws_team_name
           LEFT JOIN mclachbot_teams T2
           ON W.opponent = T2.ws_team_name
           LEFT JOIN mclachbot_leagues L1
           ON W.competition = L1.ws_league_name
           WHERE W.matchId={match_id} AND T1.gender='m'
    
    """
    data1 = conn.wsquery(query1)
    data1["sub_id"] = 1
    data2 = conn.wsquery(query2)

    data = pd.concat([data1, data2])
    data["sub_id"] = data["sub_id"].fillna(1)
    data = data.sort_values(["period", "minute", "second", "eventId", "sub_id"])
    data=attach_positional_data(conn, data)
    return data


class Funnel(ABC):
    @classmethod
    @abstractmethod
    def apply(cls, df: pd.DataFrame, **kwargs):
        """
        Apply the funnel to the dataframe

        Args:
            df (pd.DataFrame): _description_
            **kwargs: _description_

        Returns:
            _description_
        """

    @classmethod
    @abstractmethod
    def get_name(cls) -> str:
        """
        Get the name of the funnel.

        Returns:
            str: _description_
        """


class DataFrameFunnelDataSource:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_data(self, data_source_name: str, **kwargs):
        # Find the appropriate funnel class and run the
        # data through the apply method
        try:
            subclass = next(c for c in get_all_subclasses(Funnel) if c.get_name() == data_source_name)
            return subclass.apply(self.df, **kwargs)
        except StopIteration as e:
            raise ValueError(f"Invalid data requester name: {data_source_name}") from e
