from footmav.utils import whoscored_funcs as WF
from footmav.event_aggregation.event_aggregator_processor import event_aggregator
from footmav.data_definitions.whoscored.constants import EventType
from footmav.event_aggregation.aggregators import (
    touches,
    xa,
    npxg,
    crosses,
    aerials,
    ground_duels_won,
    ground_duels_lost,
)
from footballdashboardsdata.funnels.funnel_api import Funnel
import numpy as np
import pandas as pd


@event_aggregator(suffix="")
def progressive_pass_distance(dataframe):
    return np.maximum(
        0,
        np.nan_to_num(
            WF.progressive_distance(dataframe)
            * (dataframe["event_type"] == EventType.Pass)
            * (dataframe["outcomeType"] == 1),
            0,
        ),
    )


@event_aggregator(suffix="")
def progressive_carry_distance(dataframe):
    return np.maximum(
        0,
        np.nan_to_num(
            WF.progressive_distance(dataframe)
            * (dataframe["event_type"] == EventType.Carry)
            * (dataframe["outcomeType"] == 1),
            0,
        ),
    )


@event_aggregator(suffix="")
def open_play_passes_completed_into_the_box(dataframe):
    return (
        (dataframe["event_type"] == EventType.Pass)
        & (WF.col_has_qualifier(dataframe, qualifier_code=2))  # not cross
        & (~WF.col_has_qualifier(dataframe, qualifier_code=5))  # not free kick
        & (~WF.col_has_qualifier(dataframe, qualifier_code=6))  # not corner
        & (~WF.col_has_qualifier(dataframe, qualifier_code=107))  # not throw in
        & (~WF.col_has_qualifier(dataframe, qualifier_code=123))  # not keeper throw
        & (WF.into_attacking_box(dataframe))
    )


@event_aggregator(suffix="")
def crosses_completed_into_the_box(dataframe):
    return crosses.success(dataframe) & WF.in_attacking_box(dataframe)


@event_aggregator(suffix="")
def carries_into_the_box(dataframe):
    return (dataframe["event_type"] == EventType.Carry) & WF.into_attacking_box(dataframe)


class BallProgressionButterflyFunnel(Funnel):
    @classmethod
    def get_name(cls) -> str:
        return "ball_progression_butterfly"

    @classmethod
    def apply(cls, data: pd.DataFrame) -> pd.DataFrame:
        progressive_carry_distance(data)
        progressive_pass_distance(data)
        touches(data)
        home_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "home_score",
        ].iloc[0]

        away_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "away_score",
        ].iloc[0]
        match_date = data.loc[
            (data["event_type"] != EventType.Carry),
            "match_date",
        ].iloc[0]

        data["home_score"] = home_score
        data["away_score"] = away_score
        data["match_date"] = match_date

        home_team = data.loc[
            (data["is_home_team"] == True) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        away_team = data.loc[
            (data["is_home_team"] == False) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        data["home_team"] = home_team
        data["away_team"] = away_team
        grouped_progressives = (
            data.groupby(
                [
                    "player_name",
                    "team",
                    "is_home_team",
                    "match_date",
                    "home_score",
                    "away_score",
                    "home_team",
                    "away_team",
                    "decorated_league_name",
                ]
            )
            .agg(
                {
                    "progressive_carry_distance": "sum",
                    "progressive_pass_distance": "sum",
                    "touches_attempted": "sum",
                    "position": "first",
                }
            )
            .reset_index()
        )
        grouped_progressives["progressive_distance"] = (
            grouped_progressives["progressive_carry_distance"] + grouped_progressives["progressive_pass_distance"]
        )
        grouped_progressives["progressive_distance_per_touch"] = (
            grouped_progressives["progressive_distance"]
            / grouped_progressives["touches_attempted"]
            * (grouped_progressives["touches_attempted"] > 5)
        )
        grouped_progressives = grouped_progressives.loc[grouped_progressives["progressive_distance"] > 30]
        return grouped_progressives


class ExpectedGoalContributionButterflyFunnel(Funnel):
    @classmethod
    def get_name(cls) -> str:
        return "expected_goal_contributions_butterfly"

    @classmethod
    def apply(cls, data: pd.DataFrame) -> pd.DataFrame:
        xa(data)
        npxg(data)
        touches(data)
        home_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "home_score",
        ].iloc[0]

        away_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "away_score",
        ].iloc[0]
        match_date = data.loc[
            (data["event_type"] != EventType.Carry),
            "match_date",
        ].iloc[0]

        data["home_score"] = home_score
        data["away_score"] = away_score
        data["match_date"] = match_date

        home_team = data.loc[
            (data["is_home_team"] == True) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        away_team = data.loc[
            (data["is_home_team"] == False) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        data["home_team"] = home_team
        data["away_team"] = away_team
        grouped = (
            data.groupby(
                [
                    "player_name",
                    "team",
                    "is_home_team",
                    "match_date",
                    "home_score",
                    "away_score",
                    "home_team",
                    "away_team",
                    "decorated_league_name",
                ]
            )
            .agg(
                {
                    "xa": "sum",
                    "shooting_npxg": "sum",
                    "touches_attempted": "sum",
                    "position": "first",
                }
            )
            .rename(
                columns={
                    "xa": "expected_assists",
                    "shooting_npxg": "non_penalty_expected_goals",
                }
            )
            .reset_index()
        )
        grouped["expected_goal_contributions"] = grouped["expected_assists"] + grouped["non_penalty_expected_goals"]
        grouped["expected_goal_contributions_per_touch"] = (
            grouped["expected_goal_contributions"] / grouped["touches_attempted"] * (grouped["touches_attempted"] > 5)
        )
        grouped = grouped.loc[grouped["expected_goal_contributions"] > 0]
        return grouped


class ChanceCreationButterflyFunnel(Funnel):
    @classmethod
    def get_name(cls) -> str:
        return "chance_creation_butterfly"

    @classmethod
    def apply(cls, data: pd.DataFrame) -> pd.DataFrame:
        open_play_passes_completed_into_the_box(data)
        carries_into_the_box(data)
        crosses_completed_into_the_box(data)
        touches(data)
        home_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "home_score",
        ].iloc[0]

        away_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "away_score",
        ].iloc[0]
        match_date = data.loc[
            (data["event_type"] != EventType.Carry),
            "match_date",
        ].iloc[0]

        data["home_score"] = home_score
        data["away_score"] = away_score
        data["match_date"] = match_date

        home_team = data.loc[
            (data["is_home_team"] == True) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        away_team = data.loc[
            (data["is_home_team"] == False) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        data["home_team"] = home_team
        data["away_team"] = away_team
        grouped = (
            data.groupby(
                [
                    "player_name",
                    "team",
                    "is_home_team",
                    "match_date",
                    "home_score",
                    "away_score",
                    "home_team",
                    "away_team",
                    "decorated_league_name",
                ]
            )
            .agg(
                {
                    "crosses_completed_into_the_box": "sum",
                    "open_play_passes_completed_into_the_box": "sum",
                    "carries_into_the_box": "sum",
                    "touches_attempted": "sum",
                    "position": "first",
                }
            )
            .reset_index()
        )
        grouped["successful_actions_into_box"] = (
            grouped["crosses_completed_into_the_box"]
            + grouped["open_play_passes_completed_into_the_box"]
            + grouped["carries_into_the_box"]
        )
        grouped["successful_deliveries_into_penalty_box"] = (
            grouped["successful_actions_into_box"] / grouped["touches_attempted"] * (grouped["touches_attempted"] > 5)
        )
        grouped = grouped.loc[grouped["successful_actions_into_box"] > 0]
        return grouped


class DuelsButterflyFunnel(Funnel):
    @classmethod
    def get_name(cls) -> str:
        return "duels_butterfly"

    @classmethod
    def apply(cls, data: pd.DataFrame) -> pd.DataFrame:
        ground_duels_won(data)
        ground_duels_lost(data)
        data["aerials_won"] = aerials.success(data)
        aerials(data)
        home_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "home_score",
        ].iloc[0]

        away_score = data.loc[
            (data["event_type"] != EventType.Carry),
            "away_score",
        ].iloc[0]
        match_date = data.loc[
            (data["event_type"] != EventType.Carry),
            "match_date",
        ].iloc[0]

        data["home_score"] = home_score
        data["away_score"] = away_score
        data["match_date"] = match_date

        home_team = data.loc[
            (data["is_home_team"] == True) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        away_team = data.loc[
            (data["is_home_team"] == False) & (data["event_type"] != EventType.Carry),
            "decorated_team_name",
        ].iloc[0]
        data["home_team"] = home_team
        data["away_team"] = away_team
        grouped = (
            data.groupby(
                [
                    "player_name",
                    "team",
                    "is_home_team",
                    "match_date",
                    "home_score",
                    "away_score",
                    "home_team",
                    "away_team",
                    "decorated_league_name",
                ]
            )
            .agg(
                {
                    "ground_duels_won": "sum",
                    "ground_duels_lost": "sum",
                    "aerials_attempted": "sum",
                    "aerials_won": "sum",
                    "position": "first",
                }
            )
            .rename(
                columns={
                    "aerials_won": "aerial_duels_won",
                    "aerials_attempted": "aerial_duels_attempted",
                }
            )
            .reset_index()
        )
        grouped["total_duels_attempted"] = (
            grouped["ground_duels_won"] + grouped["ground_duels_lost"] + grouped["aerial_duels_attempted"]
        )
        grouped["total_duels_won"] = grouped["ground_duels_won"] + grouped["aerial_duels_won"]
        grouped["duel_win_percentage"] = grouped["total_duels_won"] / grouped["total_duels_attempted"]
        grouped = grouped.loc[grouped["total_duels_won"] > 0]
        return grouped
