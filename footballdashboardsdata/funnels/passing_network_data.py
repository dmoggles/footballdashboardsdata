from footballdashboardsdata.funnels.funnel_api import Funnel
import pandas as pd
from typing import Tuple


class PassingNetworkFunnel(Funnel):
    @classmethod
    def get_name(cls) -> str:
        return "pass_network"

    @classmethod
    def apply(
        cls, df: pd.DataFrame, team: str, formation: str, pitch_half: str = "", minutes_range: Tuple[int, int] = None
    ) -> pd.DataFrame:
        data = df.copy()
        data = data.loc[(data["team"] == team) & (data["formation"] == formation)]
        if pitch_half:
            assert pitch_half in ["attacking", "defensive"], "pitch_half must be either 'attacking' or 'defensive'"
            if pitch_half == "attacking":
                data = data.loc[data["x"] >= 50]
            else:
                data = data.loc[data["x"] <= 50]
        if minutes_range:
            assert len(minutes_range) == 2, "minutes_range must be a tuple of length 2"
            assert minutes_range[0] < minutes_range[1], "first element of minutes_range must be smaller than second"
            assert minutes_range[0] >= 0, "first element of minutes_range must be greater than or equal to 0"
            data = data.loc[(data["minute"] >= minutes_range[0]) & (data["minute"] <= minutes_range[1])]

        return data
