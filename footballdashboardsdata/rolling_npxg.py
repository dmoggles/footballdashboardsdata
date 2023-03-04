import pandas as pd
from footballdashboardsdata.datasource import DataSource
from footmav import FbRefData, fb, aggregate_by, filter, filters, Filter
from dbconnect.connector import Connection

class RollingNPXGDataSource(DataSource):
    def impl_get_data(self, team:str, league:str, season:int, rolling_window:int, normalized:bool)->pd.DataFrame:
        """
        Joins team and opponent nxpg data and computes rolling averages on each
        """
        conn = Connection('M0neyMa$e')
        raw_data = conn.query(f"""
        SELECT * FROM fbref WHERE season={season}
        AND comp='{league}'
        AND (squad='{team}' OR opponent='{team}')
        """)
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
        avg_data = conn.query(f"""SELECT squad, opponent, player, date, match_id, npxg, home FROM fbref WHERE season={season} and comp='{league}'""")
        league_avg = avg_data.groupby(['date','squad']).agg({'npxg':'sum'}).groupby('squad').agg({'npxg':['sum','count']}).sum().values[0]/avg_data.groupby(['date','squad']).agg({'npxg':'sum'}).groupby('squad').agg({'npxg':['sum','count']}).sum().values[1]
        avg_for = avg_data.groupby(['squad','date']).agg({'npxg':'sum'}).groupby('squad').agg({'npxg':'mean'}).to_dict()['npxg'] 
        avg_against = avg_data.groupby(['opponent','date']).agg({'npxg':'sum'}).groupby('opponent').agg({'npxg':'mean'}).to_dict()['npxg']
        df['norm_for']=df['opponent'].map(avg_against)
        df['norm_against']=df['opponent'].map(avg_for)
        df['npxg_norm']=df['npxg']-df['norm_for']+league_avg
        df['npxg_opp_norm']=df['npxg_opp']-df['norm_against']+league_avg
        npxg_for_against_rolling = (
            df.set_index([fb.DATE.N, fb.OPPONENT.N])
            .rolling(window=rolling_window)
            .mean()
            .reset_index()
            .dropna()
        )
        npxg_for_against_rolling["round"] = range(0, len(npxg_for_against_rolling))
        if normalized:
            npxg_for_against_rolling = npxg_for_against_rolling[[fb.DATE.N, fb.OPPONENT.N,'npxg_norm','npxg_opp_norm','round']].rename(columns={'npxg_norm':'npxg','npxg_opp_norm':'npxg_opp'})
            
        data =  npxg_for_against_rolling[[fb.DATE.N, fb.OPPONENT.N, "npxg", "npxg_opp", "round"]]
        data['team'] = team
        data['league'] = league
        data['season'] = season
        data['rolling_window'] = rolling_window
        data['normalized'] = normalized
        return data

    @classmethod
    def get_name(cls) -> str:
        return "rolling_npxg"