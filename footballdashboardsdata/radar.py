from typing import List
import pandas as pd
from footballdashboardsdata.datasource import DataSource
import datetime as dt


class RadarDataSource(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "Radar"

    def impl_get_data(
        self,
        template_name: str,
        player_ids: List[str],
        leagues: List[str],
        teams: List[str],
        seasons: List[int],
        start_dates: List[dt.date] = None,
        end_dates: List[dt.date] = None,
        use_all_minutes: bool = False,
    ) -> pd.DataFrame:
        assert len(player_ids) == 2, "Must provide exactly 2 players"
        assert len(teams) == 2, "Must provide exactly 2 teams"
        assert len(seasons) == 2, "Must provide exactly 2 seasons"
        start_dates = start_dates or [None, None]
        end_dates = end_dates or [None, None]

        df1 = DataSource.get_data(
            template_name,
            player_id=player_ids[0],
            leagues=leagues,
            team=teams[0],
            season=seasons[0],
            use_all_minutes=use_all_minutes,
            start_date=start_dates[0],
            end_date=end_dates[0],
        )
        df2 = DataSource.get_data(
            template_name,
            player_id=player_ids[1],
            leagues=leagues,
            team=teams[1],
            season=seasons[1],
            use_all_minutes=use_all_minutes,
            start_date=start_dates[1],
            end_date=end_dates[1],
        )
        df = pd.concat([df1, df2])
        template_name = {
            "CMPizza": "Midfielder",
            "CBPizza": "Centre Back",
            "FBPizza": "Full Back",
            "FWPizza": "Forward",
            "AMPizza": "Attacking Midfielder/Winger",
            "ATTPizza": "Combined Fwd/AM",
            "GKPizza": "Goalkeeper",
            "TargetmanPizza": "Targetman Forward",
        }[template_name]
        df["Template Name"] = template_name
        return df
