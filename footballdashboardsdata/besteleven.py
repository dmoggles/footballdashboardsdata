from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection
import datetime as dt
import pandas as pd
import numpy as np
from typing import Dict
from footballdashboardsdata.utils.queries import (
    get_decorated_league_name_from_fb_name,
    get_decorated_team_name_from_fb_name,
)


class BestEleventDataSource(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "besteleven"

    INDEX_COLS = [
        "player",
        "rank_position",
        "squad",
        "opponent",
        "date",
        "comp",
        "match_id",
        "season",
        "minutes",
        "position",
        "week",
    ]

    def _get_seasons_data(
        self, league: str, season: int, start_date: dt.date = None, end_date: dt.date = None
    ) -> pd.DataFrame:
        conn = Connection("M0neyMa$e")
        sql = f"""SELECT * FROM fbref 
            WHERE comp = '{league}'
            AND season = {season}
            """
        if start_date:
            sql += f"AND date >= '{start_date}'"
        if end_date:
            sql += f"AND date <= '{end_date}'"

        data = conn.query(sql)
        data["position"] = data["position"].apply(lambda x: x.split(",")[0])

        return data

    def _get_shot_data(self, league: str, season: int, start_date: dt.date, end_date: dt.date) -> pd.DataFrame:
        conn = Connection("M0neyMa$e")

        query = f"""
        SELECT * FROM football_data.fbref_shots 
        WHERE comp='{league}'
        AND season={season}
        AND outcome='Goal'
        """
        if start_date:
            query += f"AND date >= '{start_date}'"
        if end_date:
            query += f"AND date <= '{end_date}'"

        data = conn.query(query)
        return data

    def _attach_sub_on_sub_off(self, data: pd.DataFrame):
        data["subbed_on"] = data.apply(
            lambda r: 0
            if r["game_started"] == 1
            else data.loc[data["match_id"] == r["match_id"]]["minutes"].max() - r["minutes"],
            axis=1,
        )
        data["subbed_off"] = data.apply(
            lambda r: data.loc[data["match_id"] == r["match_id"]]["minutes"].max()
            if r["game_started"] == 0
            else r["minutes"],
            axis=1,
        )

    def _attach_team_goal_conceded(self, data: pd.DataFrame, shots: pd.DataFrame):
        data["team_goals_conceded"] = data.apply(
            lambda r: len(
                shots.loc[
                    (shots["match_id"] == r["match_id"])
                    & (shots["squad"] != r["squad"])
                    & (shots["minute"] > r["subbed_on"] if r["subbed_on"] > 0 else True)
                    & (shots["minute"] <= r["subbed_off"])
                ]
            ),
            axis=1,
        )

    def _get_scores(self) -> pd.DataFrame:
        conn = Connection("M0neyMa$e")
        data = conn.query("SELECT * FROM power_ranking_reference")
        return data

    def _transform_position(self, position: str):
        pos = position.split(",")[0]
        if pos in ["LB", "RB", "WB", "RWB", "LWB"]:
            return "FB"
        if pos in ["LW", "RW", "LM", "RM"]:
            return "WFW"
        if pos in ["AM"]:
            return "AMF"
        if pos in ["CM", "DM"]:
            return "MF"
        return pos

    def _attach_scores(self, data, scores):
        PASS_AVG_THRESHOLD = 0.75
        data["rank_position"] = data["enriched_position"].apply(self._transform_position)
        data["position"] = data["enriched_position"].apply(lambda x: x.split(",")[0])

        attributes = scores["data_attribute"].unique()

        data["psxg"] = data["psxg_gk"] - data["goals_conceded"]
        data["clean_sheets"] = data.apply(
            lambda r: 1 if r["goals_conceded"] == 0 and r["shots_on_target_against"] >= 3 else 0, axis=1
        )
        data["save_pct"] = data.apply(
            lambda r: max(0, r["shots_on_target_against"] - r["goals_conceded"]) / r["shots_on_target_against"]
            if r["shots_on_target_against"] > 0
            else 0,
            axis=1,
        ).fillna(0)
        data["crosses_stopped_pct"] = (data["crosses_stopped_gk"] / data["crosses_gk"]).fillna(0)
        data["pass_completed_pct"] = data.apply(
            lambda x: 0 if x["passes"] <= 5 else x["passes_completed"] / x["passes"] - PASS_AVG_THRESHOLD, axis=1
        )

        data = data[self.INDEX_COLS + list(attributes)]
        data = data.fillna(0)
        data = data.set_index(self.INDEX_COLS)
        data = data.stack().rename_axis(self.INDEX_COLS + ["var_name"]).rename("value").reset_index()
        data = pd.merge(
            data,
            scores[["position", "data_attribute", "score", "data_category"]],
            left_on=["rank_position", "var_name"],
            right_on=["position", "data_attribute"],
            suffixes=("", "_scores"),
            how="inner",
        )
        data["earned_score"] = data["value"] * data["score"]
        data = data[data["earned_score"] != 0]
        return data[
            [
                "player",
                "squad",
                "opponent",
                "date",
                "comp",
                "match_id",
                "season",
                "minutes",
                "position",
                "rank_position",
                "week",
                "data_attribute",
                "data_category",
                "value",
                "score",
                "earned_score",
            ]
        ]

    def _possession_adjust_factors(self, data: pd.DataFrame) -> Dict[str, float]:
        touches_for = (
            data.groupby(["squad", "match_id"])
            .agg({"touches": "sum"})
            .groupby("squad")
            .agg({"touches": "mean"})
            .reset_index()
        )
        touches_against = (
            data.groupby(["opponent", "match_id"])
            .agg({"touches": "sum"})
            .groupby("opponent")
            .agg({"touches": "mean"})
            .reset_index()
            .rename(columns={"opponent": "squad"})
        )
        merged = pd.merge(left=touches_for, right=touches_against, how="left", on="squad", suffixes=("", "_against"))
        merged["padj_factor"] = 0.5 / (merged["touches_against"] / (merged["touches_against"] + merged["touches"]))
        return merged.set_index("squad")["padj_factor"].to_dict()

    def _pivot_ranking_data(self, data):
        data = (
            data.pivot(index=self.INDEX_COLS + ["data_attribute"], values="earned_score", columns=["data_category"])
            .fillna(0)
            .groupby(self.INDEX_COLS)
            .sum()
        )
        data["total"] = data.sum(axis=1)
        return data.reset_index().reset_index(drop=True)

    def _aggregated_position(self, df: pd.DataFrame) -> pd.Series:
        return df["rank_position"].map(
            {"CB": "CB", "FB": "FB", "MF": "MF", "AMF": "MF", "WFW": "FW", "FW": "FW", "GK": "GK"}
        )

    def _find_position_fit(self, dt: pd.DataFrame) -> str:
        value_counts = dt.value_counts()
        if value_counts.iloc[0] / len(dt) >= (2.0 / 3.0 - 1e-6):
            return value_counts.index[0]
        else:
            return ""

    def _generate_team_of_the_period(self, agg_rankings: pd.DataFrame) -> pd.DataFrame:
        gk_pool = agg_rankings.loc[agg_rankings["agg_position"] == "GK"]
        team_selection = gk_pool.sort_values("total", ascending=False).head(1)
        fb_pool = agg_rankings.loc[agg_rankings["agg_position"] == "FB"]
        fb_selection = fb_pool.sort_values("total", ascending=False).head(1)
        team_selection = pd.concat([team_selection, fb_selection])
        cb_pool = agg_rankings.loc[agg_rankings["agg_position"] == "CB"]
        team_selection = pd.concat([team_selection, cb_pool.sort_values("total", ascending=False).head(2)])
        fb_cb_pool = agg_rankings.loc[
            (agg_rankings["agg_position"].isin(["FB", "CB"]))
            & (~agg_rankings["player"].isin(team_selection["player"]))
            & (~agg_rankings["position"].apply(lambda x: x[0]).isin([fb_selection["position"].iloc[0][0]]))
        ]
        team_selection = pd.concat([team_selection, fb_cb_pool.sort_values("total", ascending=False).head(1)])
        midfielders = agg_rankings.loc[agg_rankings["agg_position"] == "MF"]
        am_cm_pool = midfielders.loc[midfielders["position"].apply(lambda x: "AM" in x or "CM" in x)]
        am_cm_selection = am_cm_pool.sort_values("total", ascending=False).head(1)
        team_selection = pd.concat([team_selection, am_cm_selection])
        dm_cm_pool = midfielders.loc[
            (midfielders["position"].apply(lambda x: "DM" in x or "CM" in x))
            & (~midfielders["player"].isin(am_cm_selection["player"]))
        ]
        dm_cm_selection = dm_cm_pool.sort_values("total", ascending=False).head(1)
        team_selection = pd.concat([team_selection, dm_cm_selection])
        mf_pool = midfielders.loc[~midfielders["player"].isin(team_selection["player"])]
        team_selection = pd.concat([team_selection, mf_pool.sort_values("total", ascending=False).head(1)])
        forwards = agg_rankings.loc[agg_rankings["agg_position"] == "FW"]
        st_pool = forwards.loc[forwards["position"].apply(lambda x: x[0] == "FW")]
        st_selection = st_pool.sort_values("total", ascending=False).head(1)
        team_selection = pd.concat([team_selection, st_selection])
        wing_pool = forwards.loc[forwards["position"].apply(lambda x: x[0] in ["LW", "RW", "LM", "RM"])]
        wing_selection = wing_pool.sort_values("total", ascending=False).head(1)
        team_selection = pd.concat([team_selection, wing_selection])
        third_forward_pool = wing_pool.loc[~wing_pool["player"].isin(team_selection["player"])]
        team_selection = pd.concat([team_selection, third_forward_pool.sort_values("total", ascending=False).head(1)])
        team_selection["position"] = team_selection["position"].apply(lambda x: x[0])
        return team_selection

    def _attach_best_attributes_totw(self, totw: pd.DataFrame, full_scores: pd.DataFrame) -> pd.DataFrame:
        total_stats = [
            "goals",
            "opening_goals",
            "clean_sheets",
            "equalising_goals",
            "winning_goals",
            "assists",
            "pass_completed_pct",
        ]
        for i, r in totw.iterrows():
            player_scores = full_scores.loc[
                (full_scores["squad"] == r["squad"]) & (full_scores["player"] == r["player"])
            ]
            player_scores = (
                player_scores.groupby(["data_attribute", "data_category"])
                .agg({"earned_score": "sum", "value": "sum", "position": lambda x: x.value_counts().index.tolist()[0]})
                .reset_index()
            )
            player_scores = player_scores.loc[~player_scores["data_attribute"].isin(["pass_completed_pct"])]
            attributes = player_scores.sort_values("earned_score", ascending=False)
            selected_attributes = []
            j = 0
            while len(selected_attributes) < 3:
                r2 = attributes.iloc[j]

                if (
                    (r2["data_attribute"] == "save_pct" and r2["value"] == 1.0)
                    or (
                        r2["data_attribute"] == "goals"
                        and len([sa for sa in selected_attributes if sa[0] in ["winning_goals", "equalising_goals"]])
                        > 0
                    )
                    or (
                        (r2["position"] in ["FB", "CB"])
                        and len(selected_attributes) == 2
                        and len([sa for sa in selected_attributes if sa[1] == "defending"]) == 0
                        and r2["data_category"] != "defending"
                    )
                ):
                    j += 1
                    continue
                if r2["data_attribute"] not in total_stats and r["matches"] > 1:
                    value = r2["value"] / r["minutes"] * 90
                    name = f"{r2['data_attribute']}_p90"
                else:
                    value = r2["value"]
                    name = r2["data_attribute"]

                selected_attributes.append((name, r2["data_category"], value))
                j += 1

            for k, (attr, _, val) in enumerate(selected_attributes):
                totw.loc[i, f"top_category_{k+1}"] = attr
                totw.loc[i, f"top_value_{k+1}"] = val

        return totw

    def _attach_position_placement(self, team: pd.DataFrame) -> pd.DataFrame:
        position_placement = {
            "GK": "",
            "LB": "",
            "LCB": "",
            "RCB": "",
            "RB": "",
            "CDM": "",
            "RCM": "",
            "LCM": "",
            "RW": "",
            "FW": "",
            "LW": "",
        }
        team["position"] = team["position"].apply(lambda x: x.replace("WB", "B"))
        totw = team.sort_values("total", ascending=True)
        position_placement["GK"] = totw.loc[totw["rank_position"] == "GK", "player"].iloc[0]

        cbs = totw.loc[totw["rank_position"] == "CB", "player"].tolist()

        if "RB" in totw["position"].tolist():
            position_placement["RB"] = totw.loc[totw["position"] == "RB", "player"].iloc[0]
        else:
            position_placement["RB"] = cbs.pop()
        if "LB" in totw["position"].tolist():
            position_placement["LB"] = totw.loc[totw["position"] == "LB", "player"].iloc[0]
        else:
            position_placement["LB"] = cbs.pop()
        position_placement["LCB"] = cbs.pop()
        position_placement["RCB"] = cbs.pop()
        cms = totw.loc[totw["position"].isin(["CM", "LM", "RM"]), "player"].tolist()
        if "DM" in totw["position"].tolist():
            position_placement["CDM"] = totw.loc[totw["position"] == "DM", "player"].iloc[0]
        elif "CM" in totw["position"].tolist():
            totw_sorted = totw.sort_values("defending", ascending=False)

            position_placement["CDM"] = totw_sorted.loc[
                totw_sorted["position"].isin(["LM", "RM", "CM"]), "player"
            ].iloc[0]
        else:
            position_placement["CDM"] = cms.pop()
        cms = totw.loc[
            (totw["position"].isin(["CM", "AM", "LM", "RM", "DM"])) & (totw["player"] != position_placement["CDM"]),
            "player",
        ].tolist()

        position_placement["LCM"] = cms.pop()
        position_placement["RCM"] = cms.pop()
        fws = totw.loc[totw["rank_position"] == "FW", "player"].tolist()
        rws = totw.loc[totw["position"] == "RW", "player"].tolist()
        lws = totw.loc[totw["position"] == "LW", "player"].tolist()
        if len(lws) > 0:
            position_placement["LW"] = lws.pop()
        elif len(rws) > 1:
            position_placement["LW"] = rws.pop()
        else:
            position_placement["LW"] = fws.pop()

        if len(rws) > 0:
            position_placement["RW"] = rws.pop()
        elif len(lws) > 0:
            position_placement["RW"] = lws.pop()
        else:
            position_placement["RW"] = fws.pop()
        position_placement["ST"] = fws.pop()

        position_placement = {v: k for k, v in position_placement.items()}
        totw["placement_position"] = totw["player"].map(position_placement)
        return totw

    def _attach_ranking(self, data, ranking_baseline_data):
        for i, row in data.iterrows():
            if row["position"] == "GK":
                comparision = ranking_baseline_data[ranking_baseline_data["rank_position"] == "GK"]
                stats = ["shotstopping", "area_control", "distribution"]

            else:
                comparision = ranking_baseline_data[ranking_baseline_data["rank_position"] != "GK"]
                stats = ["defending", "finishing", "providing", "progressing"]

            for stat in stats:
                comp = comparision[comparision[stat] != 0].copy()
                data.loc[i, f"{stat}_ranking"] = len(comp[comp[stat] < row[stat]]) / len(comp)

    def impl_get_data(
        self, league: str, season: int, start_date: dt.date = None, end_date: dt.date = None, tag: str = None
    ):
        raw_data = self._get_seasons_data(league, season, start_date, end_date)
        shot_data = self._get_shot_data(league, season, start_date, end_date)
        self._attach_sub_on_sub_off(raw_data)
        self._attach_team_goal_conceded(raw_data, shot_data)
        scores = self._get_scores()
        full_scores = self._attach_scores(raw_data, scores)
        padj_full_scores = full_scores.copy()
        padj_factors = self._possession_adjust_factors(raw_data)
        for i, r in padj_full_scores.loc[
            padj_full_scores["data_attribute"].isin(
                ["blocks", "interceptions", "clearances", "tackles_won", "dribbled_past"]
            )
        ].iterrows():
            padj_full_scores.loc[i, "earned_score"] = r["earned_score"] * padj_factors[r["squad"]]
        data = self._pivot_ranking_data(padj_full_scores)
        data["matches"] = 1
        # agg_full_scores = full_scores.groupby(['player','squad','data_attribute','data_category']).agg({'earned_score':'sum','value':'sum','position':lambda x: x.value_counts().index.tolist()[0]}).reset_index().sort_values(['player','earned_score'])
        data["agg_position"] = self._aggregated_position(data)
        aggregated = data.groupby(["player", "squad"]).agg(
            {
                "minutes": "sum",
                "matches": "count",
                "defending": "sum",
                "finishing": "sum",
                "shotstopping": "sum",
                "providing": "sum",
                "progressing": "sum",
                "total": "sum",
                "area_control": "sum",
                "distribution": "sum",
                "position": lambda x: x.value_counts().index.tolist()[0:1],
                "agg_position": self._find_position_fit,
                "rank_position": lambda x: x.value_counts().index.tolist()[0],
            }
        )
        for c in [
            "defending",
            "finishing",
            "shotstopping",
            "providing",
            "progressing",
            "total",
            "area_control",
            "distribution",
        ]:
            aggregated[c] = aggregated[c] / np.sqrt(aggregated["matches"])
        aggregated.sort_values("total")
        best_11 = self._generate_team_of_the_period(aggregated.reset_index())
        best_11 = self._attach_best_attributes_totw(best_11, full_scores)
        best_11 = self._attach_position_placement(best_11)
        self._attach_ranking(best_11, aggregated)
        best_11["season"] = season
        best_11["league"] = league
        best_11["league_name"] = get_decorated_league_name_from_fb_name(league)
        best_11["start_date"] = start_date
        best_11["end_date"] = end_date
        best_11["tag"] = tag
        return best_11
