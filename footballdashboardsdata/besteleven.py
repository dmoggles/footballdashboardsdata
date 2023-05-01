from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection
import datetime as dt
import pandas as pd
import numpy as np
from typing import Dict, List, Callable, Union
from footballdashboardsdata.utils.queries import (
    get_decorated_league_name_from_fb_name,
    get_decorated_team_name_from_fb_name,
)
from abc import ABC, abstractmethod


class FormationComposer(ABC):
    @classmethod
    def get(cls, formation_name: str):
        formation_name = formation_name.replace(" ", "").replace("-", "")
        try:
            subclass = next(c for c in cls.__subclasses__() if c.__name__ == f"FormationComposer{formation_name}")
            return subclass()
        except StopIteration as e:
            raise ValueError(f"Invalid formation name: {formation_name}") from e

    @classmethod
    @abstractmethod
    def get_position_lists(cls) -> List[List[str]]:
        pass

    @classmethod
    @abstractmethod
    def get_placement_functions(cls) -> Dict[str, Union[Callable[[pd.DataFrame], pd.DataFrame], List[str]]]:
        pass

    def select_team(self, data: pd.DataFrame) -> pd.DataFrame:
        data["position"] = data["position"].apply(lambda x: x[0])
        gk_pool = data.loc[data["position"] == "GK"]
        selection = gk_pool.sort_values("total", ascending=False).head(1)
        team_selection = selection.copy()
        for position_list in self.get_position_lists():
            position_pool = data.loc[
                (data["position"].isin(position_list)) & (~data["player"].isin(team_selection["player"]))
            ]
            selection = position_pool.sort_values("total", ascending=False).head(1)
            team_selection = pd.concat([team_selection, selection])
        return team_selection

    def place_team(self, data: pd.DataFrame) -> pd.DataFrame:
        data = data.reset_index()
        data["placement_position"] = ""
        for position, position_f in self.get_placement_functions().items():
            unplaced = data.loc[data["placement_position"] == ""]
            if isinstance(position_f, list):
                position_pool = unplaced.loc[unplaced["position"].isin(position_f)]
            else:
                position_pool = position_f(data)
            selection = position_pool.sort_values("total", ascending=False).head(1)

            data.loc[data["player"].isin(selection["player"]), "placement_position"] = position
        return data


class FormationComposer442(FormationComposer):
    @classmethod
    def get_position_lists(cls) -> List[List[str]]:
        return [
            ["LWB", "LB"],
            ["CB"],
            ["CB"],
            ["RB", "RWB"],
            ["LM", "LW"],
            ["DM", "CM"],
            ["CM", "AM"],
            ["RM", "RW"],
            ["FW"],
            ["FW"],
        ]

    @classmethod
    def get_placement_functions(cls) -> Dict[str, Union[Callable[[pd.DataFrame], pd.DataFrame], List[str]]]:
        return {
            "GK": ["GK"],
            "LB": ["LB", "LWB"],
            "LCB": ["CB"],
            "RCB": ["CB"],
            "RB": ["RB", "RWB"],
            "LM": ["LM", "LW"],
            "LCM": ["DM", "CM", "AM"],
            "RCM": ["CM", "AM", "DM"],
            "RM": ["RM", "RW"],
            "LCF": ["FW"],
            "RCF": ["FW"],
        }


class FormationComposer424(FormationComposer):
    @classmethod
    def get_position_lists(cls) -> List[List[str]]:
        return [
            ["LWB", "LB"],
            ["CB"],
            ["CB"],
            ["RB", "RWB"],
            ["LM", "LW"],
            ["DM", "CM"],
            ["CM", "AM"],
            ["RM", "RW"],
            ["FW"],
            ["FW"],
        ]

    @classmethod
    def get_placement_functions(cls) -> Dict[str, Union[Callable[[pd.DataFrame], pd.DataFrame], List[str]]]:
        return {
            "GK": ["GK"],
            "LB": ["LB", "LWB"],
            "LCB": ["CB"],
            "RCB": ["CB"],
            "RB": ["RB", "RWB"],
            "LWF": ["LM", "LW"],
            "LCDM": ["DM", "CM", "AM"],
            "RCDM": ["CM", "AM", "DM"],
            "RWF": ["RM", "RW"],
            "LF": ["FW"],
            "RF": ["FW"],
        }


class FormationComposer433(FormationComposer):
    @staticmethod
    def _get_dm(df: pd.DataFrame) -> pd.DataFrame:
        dms = df.loc[df["position"].isin(["DM"])]
        dms = dms.sort_values("defending", ascending=False)
        if len(dms) > 0:
            return dms.head(1)

        all_cms = df.loc[df["position"].isin(["CM", "AM"])]
        all_cms = all_cms.sort_values("defending", ascending=False)
        return all_cms.head(1)

    @staticmethod
    def _get_lw(df: pd.DataFrame) -> pd.DataFrame:
        lws = df.loc[df["position"].isin(["LW", "LM"])]

        if len(lws) > 0:
            return lws.head(1)

        all_wingers = df.loc[df["position"].isin(["RW", "RM"])]

        return all_wingers.head(1)

    @classmethod
    def get_position_lists(cls) -> List[List[str]]:
        return [
            ["LWB", "LB"],
            ["CB"],
            ["CB"],
            ["RB", "RWB"],
            ["DM", "CM"],
            ["CM", "AM"],
            ["DM", "CM", "AM"],
            ["RM", "RW", "LM", "LW"],
            ["RM", "RW", "LM", "LW"],
            ["FW"],
        ]

    @classmethod
    def get_placement_functions(cls) -> Dict[str, Union[Callable[[pd.DataFrame], pd.DataFrame], List[str]]]:
        return {
            "GK": ["GK"],
            "LB": ["LB", "LWB"],
            "LCB": ["CB"],
            "RCB": ["CB"],
            "RB": ["RB", "RWB"],
            "CDM": cls._get_dm,
            "LCM": ["CM", "AM", "DM"],
            "RCM": ["CM", "AM", "DM"],
            "LW": cls._get_lw,
            "RW": ["LW", "LM", "RM", "RW"],
            "ST": ["FW"],
        }


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
        self,
        league: str,
        season: int,
        start_date: dt.date = None,
        end_date: dt.date = None,
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

    def _get_scores(self, multi_leg: bool = False) -> pd.DataFrame:
        conn = Connection("M0neyMa$e")
        data = conn.query("SELECT * FROM power_ranking_reference")
        if multi_leg:
            data.loc[data["data_attribute"] == "winning_goals", "score"] = 0
            data.loc[data["data_attribute"] == "opening_goals", "score"] = 0
            data.loc[data["data_attribute"] == "equalising_goals", "score"] = 0
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
        self,
        league: str,
        season: int,
        start_date: dt.date = None,
        end_date: dt.date = None,
        tag: str = None,
        formation: str = "442",
        multi_leg: bool = False,
    ):
        formation_composer = FormationComposer.get(formation)
        raw_data = self._get_seasons_data(league, season, start_date, end_date)
        shot_data = self._get_shot_data(league, season, start_date, end_date)
        self._attach_sub_on_sub_off(raw_data)
        self._attach_team_goal_conceded(raw_data, shot_data)
        scores = self._get_scores(multi_leg=multi_leg)
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
        aggregated = aggregated.loc[aggregated["matches"] >= aggregated["matches"].max() / 2]
        aggregated.sort_values("total")

        best_11 = formation_composer.select_team(aggregated.reset_index())
        best_11 = self._attach_best_attributes_totw(best_11, full_scores)
        best_11 = formation_composer.place_team(best_11)
        self._attach_ranking(best_11, aggregated)
        best_11["season"] = season
        best_11["league"] = league
        best_11["league_name"] = get_decorated_league_name_from_fb_name(league)
        best_11["start_date"] = start_date
        best_11["end_date"] = end_date
        best_11["tag"] = tag
        best_11["formation"] = formation
        return best_11
