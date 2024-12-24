import pandas as pd
from abc import abstractmethod
from typing import Any, Dict, Tuple
import datetime as dt
from footballdashboardsdata.datasource import DataSource
from footmav import FbRefData, fb, aggregate_by, filter, filters, Filter
from dbconnect.connector import Connection
from footballdashboardsdata.utils.queries import (
    get_decorated_team_name_from_fb_name,
    get_decorated_league_name_from_fb_name,
)




class RollingNPXGDataSourceBase(DataSource):

    @abstractmethod
    def get_raw_data(self, query_params:Dict[str, Any])->Tuple[pd.DataFrame, pd.DataFrame]:
        pass


    def _impl_get_data(
        self,param_kwargs:Dict[str, Any], league:str, team:str, rolling_window: int, normalized: bool
    ) -> pd.DataFrame:
        """
        Joins team and opponent nxpg data and computes rolling averages on each
        """
        param_kwargs['league'] = league
        param_kwargs['team'] = team
        raw_data, avg_data  = self.get_raw_data(param_kwargs)
        data = FbRefData(raw_data)
        team_data = data.pipe(filter, [Filter(fb.TEAM, team, filters.EQ)]).pipe(
            aggregate_by, [fb.DATE]
        )
        opp_data = data.pipe(filter, [Filter(fb.OPPONENT, team, filters.EQ)]).pipe(
            aggregate_by, [fb.DATE]
        )
        df = pd.merge(
            left=team_data.df, right=opp_data.df, on=[fb.DATE.N], suffixes=("", "_opp")
        )[[fb.DATE.N, fb.OPPONENT.N, fb.NPXG.N, fb.NPXG.N + "_opp"]]
        
        league_avg = (
            avg_data.groupby(["date", "squad"])
            .agg({"npxg": "sum"})
            .groupby("squad")
            .agg({"npxg": ["sum", "count"]})
            .sum()
            .values[0]
            / avg_data.groupby(["date", "squad"])
            .agg({"npxg": "sum"})
            .groupby("squad")
            .agg({"npxg": ["sum", "count"]})
            .sum()
            .values[1]
        )
        avg_for = (
            avg_data.groupby(["squad", "date"])
            .agg({"npxg": "sum"})
            .groupby("squad")
            .agg({"npxg": "mean"})
            .to_dict()["npxg"]
        )
        avg_against = (
            avg_data.groupby(["opponent", "date"])
            .agg({"npxg": "sum"})
            .groupby("opponent")
            .agg({"npxg": "mean"})
            .to_dict()["npxg"]
        )
        df["norm_for"] = df["opponent"].map(avg_against)
        df["norm_against"] = df["opponent"].map(avg_for)
        df["npxg_norm"] = df["npxg"] - df["norm_for"] + league_avg
        df["npxg_opp_norm"] = df["npxg_opp"] - df["norm_against"] + league_avg
        npxg_for_against_rolling = (
            df.set_index([fb.DATE.N, fb.OPPONENT.N])
            .rolling(window=rolling_window)
            .mean()
            .reset_index()
            .dropna()
        )
        npxg_for_against_rolling["round"] = range(0, len(npxg_for_against_rolling))
        if normalized:
            npxg_for_against_rolling = npxg_for_against_rolling[
                [fb.DATE.N, fb.OPPONENT.N, "npxg_norm", "npxg_opp_norm", "round"]
            ].rename(columns={"npxg_norm": "npxg", "npxg_opp_norm": "npxg_opp"})

        data = npxg_for_against_rolling[
            [fb.DATE.N, fb.OPPONENT.N, "npxg", "npxg_opp", "round"]
        ]
        seasons = ', '.join([str(s) for s in raw_data['season'].unique()])
        data["team"] = get_decorated_team_name_from_fb_name(team, league)
        data["team_img"] = team
        data["league"] = get_decorated_league_name_from_fb_name(league)
        data["season"] = seasons
        data["rolling_window"] = rolling_window
        data["normalized"] = normalized
        return data


class RollingNPXGBySeasonDataSource(RollingNPXGDataSourceBase):
    def impl_get_data(
        self, team: str, league: str, season: int, rolling_window: int, normalized: bool
    ) -> pd.DataFrame:
        kwargs = {
            "season": season,
            
        }
        return self._impl_get_data(kwargs, league, team, rolling_window=rolling_window, normalized=normalized)

    def get_raw_data(self, query_params:Dict[str, Any])->Tuple[pd.DataFrame, pd.DataFrame]:
        conn = Connection("M0neyMa$e")
        season = query_params["season"]
        league = query_params["league"]
        team = query_params["team"]

        raw_data = conn.query(
            f"""
        SELECT * FROM fbref WHERE season={season}
        AND comp='{league}'
        AND (squad='{team}' OR opponent='{team}')
        """
        )
        avg_data = conn.query(
            f"""SELECT squad, opponent, player, date, match_id, npxg, home FROM fbref WHERE season={season} and comp='{league}'"""
        )
        return raw_data, avg_data
    @classmethod
    def get_name(cls) -> str:
        return "rolling_npxg"

class RollingNPXGByDateDataSource(RollingNPXGDataSourceBase):
    def impl_get_data(
        self, team: str, league: str,  start_date: dt.datetime, end_date: dt.datetime, rolling_window: int, normalized: bool
    ) -> pd.DataFrame:
        kwargs = {
            "start_date": start_date,
            "end_date": end_date
        }
        return self._impl_get_data(kwargs, league, team, rolling_window=rolling_window, normalized=normalized)
    
    def get_raw_data(self, query_params:Dict[str, Any])->Tuple[pd.DataFrame, pd.DataFrame]:
        conn = Connection("M0neyMa$e")
        league = query_params["league"]
        team = query_params["team"]
        start_date = query_params["start_date"]
        end_date = query_params["end_date"]

        raw_data = conn.query(
            f"""
        SELECT * FROM fbref WHERE comp='{league}'
        AND (squad='{team}' OR opponent='{team}')
        AND date BETWEEN '{start_date}' AND '{end_date}'
        """
        )

        avg_data = conn.query(
            f"""
            SELECT squad, opponent, date, match_id, npxg, home FROM fbref WHERE comp='{league}' AND date BETWEEN '{start_date}' AND '{end_date}'
            """
        )
        return raw_data, avg_data
    
    @classmethod
    def get_name(cls) -> str:
        return "rolling_npxg_by_date"
