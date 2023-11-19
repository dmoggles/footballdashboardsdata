import pandas as pd
from footmav.data_definitions.whoscored.constants import EventType
from footmav.utils import whoscored_funcs as WF
from footballdashboardsdata.funnels.funnel_api import Funnel


class SingleMatchShotFunnel(Funnel):
    @classmethod
    def get_name(cls) -> str:
        return "single_match_shots"

    @classmethod
    def apply(cls, df: pd.DataFrame):
        df = df.loc[
            df["event_type"].isin(
                [
                    EventType.ShotOnPost,
                    EventType.MissedShots,
                    EventType.SavedShot,
                    EventType.Goal,
                ]
            )
        ].copy()
        df["x"] = df.apply(
            lambda r: r["x"] if r["is_home_team"] else 100 - r["x"], axis=1
        )
        df["y"] = df.apply(
            lambda r: r["y"] if r["is_home_team"] else 100 - r["y"], axis=1
        )

        return df
