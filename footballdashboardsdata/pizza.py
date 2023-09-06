from dbconnect.connector import Connection
import pandas as pd
from typing import List
from footmav import FbRefData, fb, aggregate_by, filter, filters, Filter, per_90
from footmav.operations.possession_adjust import possession_adjust
from footballdashboardsdata.datasource import DataSource
from footballdashboardsdata.utils import possession_adjust
from footballmodels.definitions.templates import (
    MFTemplate,
    CBTemplate,
    FBTemplate,
    AttackerTemplate,
    GoalkeeperTemplate,
    TeamTemplate,
    TargetmanTemplate,
    TemplateAttribute,
    PossessionAdjustment,
)
from footballdashboardsdata.utils.queries import (
    get_decorated_team_name_from_fb_name,
    get_multiple_decorated_league_names_from_fb_names,
)

from abc import abstractmethod
from footmav.data_definitions.base import IntDataAttribute
from footmav.data_definitions.data_sources import DataSource as DataSourceEnum
from footmav.data_definitions.base import RegisteredAttributeStore

if not "self_created_shots" in [a.N for a in RegisteredAttributeStore.get_registered_attributes()]:
    SELF_CREATED_SHOTS = IntDataAttribute(name="self_created_shots", source=DataSourceEnum.FBREF)
    OPEN_PLAY_SCA_FOR_OTHERS = IntDataAttribute(name="open_play_sca_for_others", source=DataSourceEnum.FBREF)


class PizzaDataSource(DataSource):
    @abstractmethod
    def get_template(self) -> List[TemplateAttribute]:
        pass

    @abstractmethod
    def get_comparison_positions(self) -> List[str]:
        pass

    def _specific_position_impl(self, data: pd.DataFrame) -> dict:
        data_value = {
            f"{attrib.name}__value": attrib.calculation(data).apply(lambda x: f"{x:.{attrib.sig_figs}f}")
            for attrib in self.get_template()
        }
        data_rank = {
            attrib.name: attrib.calculation(data).rank(pct=True, method="min", ascending=attrib.ascending_rank)
            for attrib in self.get_template()
        }
        return {**data_value, **data_rank}

    def get_data_dict(self, data):
        specific_data = self._specific_position_impl(data)
        data_dict = {
            "Player": data[fb.PLAYER.N].tolist(),
            "Team": data[fb.TEAM.N].tolist(),
            "Minutes": data[fb.MINUTES.N].tolist(),
            "Competition": data[fb.COMPETITION.N].tolist(),
            "Season": data[fb.YEAR.N].tolist(),
        }
        data_dict.update(specific_data)
        return data_dict

    def _get_decorated_team_name(self, team_name: str, gender: str) -> str:
        query = f"""
        SELECT decorated_name FROM mclachbot_teams WHERE team_name = '{team_name}'
        AND gender='{gender}'
        """
        result = Connection("M0neyMa$e").query(query)
        if len(result) == 0:
            return team_name
        return result["decorated_name"].values[0]

    def _get_decorated_league_name(self, league_name: str) -> str:
        query = f"""
        SELECT decorated_name FROM mclachbot_leagues WHERE league_name = '{league_name}'
        """
        result = Connection("M0neyMa$e").query(query)
        if len(result) == 0:
            return league_name
        return result["decorated_name"].values[0]

    def impl_get_data(
        self, player_name: str, leagues: List[str], team: str, season: int, use_all_minutes: bool = False
    ) -> pd.DataFrame:
        template = self.get_template()
        all_template_columns = [attr.columns_used for attr in template]
        all_template_columns = [item for sublist in all_template_columns for item in sublist]
        all_template_columns = list(set(all_template_columns))
        all_columns = [
            "T1." + fb.PLAYER_ID.N,
            "T1." + fb.PLAYER.N,
            "T1." + fb.DATE.N,
            "T1." + fb.TEAM.N,
            "T1." + fb.OPPONENT.N,
            "T1." + fb.MINUTES.N,
            "T1." + fb.COMPETITION.N,
            "T1." + fb.YEAR.N,
            "T1." + fb.ENRICHED_POSITION.N,
            "T1." + fb.TOUCHES.N,
            "T1.gender",
            "T1.match_id",
            "T1.dob",
        ] + all_template_columns
        league_str = ",".join([f"'{league}'" for league in leagues])
        query = f"""
        SELECT {','.join(all_columns)}
        FROM football_data.fbref T1
        
        WHERE T1.comp in ({league_str}) AND T1.season = {season}
        """
        conn = Connection("M0neyMa$e")
        orig_df = conn.query(query)
        shot_agg_data = conn.query(
            f"""
        SELECT * FROM derived.fbref_shot_aggregations T1
        WHERE
        T1.comp in ({league_str}) AND T1.season = {season}
        """
        )
        orig_df = pd.merge(orig_df, shot_agg_data, on=["match_id", "squad", "player"], how="left", suffixes=("", "_y"))
        orig_df = orig_df.drop([col for col in orig_df.columns if col.endswith("_y")], axis=1)
        gender = orig_df["gender"].iloc[0]

        adjust_factors = possession_adjust.adj_possession_factors(orig_df)
        orig_df = orig_df.merge(adjust_factors, on=[fb.COMPETITION.N, fb.TEAM.N, fb.YEAR.N], how="left")
        adjusted_columns = []
        for attribute in template:
            if attribute.possession_adjust == PossessionAdjustment.IN_POSS:
                for col in attribute.columns_used:
                    if col not in adjusted_columns:
                        orig_df[col] = orig_df[col] * orig_df["in_possession_factor"]
                        adjusted_columns.append(col)
            elif attribute.possession_adjust == PossessionAdjustment.OUT_OF_POSS:
                for col in attribute.columns_used:
                    if col not in adjusted_columns:
                        orig_df[col] = orig_df[col] * orig_df["out_of_possession_factor"]
                        adjusted_columns.append(col)

        fbref_data = FbRefData(orig_df)

        non_position_restricted_player_data = fbref_data.pipe(aggregate_by, [fb.PLAYER_ID, fb.TEAM]).pipe(per_90).df
        non_pos_resetricted_player_df = non_position_restricted_player_data[
            non_position_restricted_player_data[fb.PLAYER.N] == player_name
        ]

        transformed_data = (
            fbref_data.pipe(
                filter,
                [
                    Filter(
                        fb.ENRICHED_POSITION,
                        self.get_comparison_positions(),
                        filters.IsIn,
                    )
                ],
            )
            .pipe(aggregate_by, [fb.PLAYER_ID, fb.TEAM])
            .pipe(per_90)
        )

        df = transformed_data.pipe(
            filter,
            [
                Filter(
                    fb.MINUTES,
                    transformed_data.df[fb.MINUTES.N].max() / 3.0,
                    filters.GTE,
                )
            ],
        ).df
        if df.loc[(df[fb.PLAYER.N] == player_name) & (df[fb.TEAM.N] == team)].shape[0] == 0:
            # if player_name not in df[fb.PLAYER.N].unique():
            df_player = transformed_data.pipe(filter, [Filter(fb.PLAYER, player_name, filters.EQ)]).df
            df = pd.concat([df, df_player])

        if use_all_minutes:
            df = df.loc[df[fb.PLAYER.N] != player_name]

            df = pd.concat([df, non_pos_resetricted_player_df])

        data_dict = self.get_data_dict(df)
        output = pd.DataFrame(data_dict)

        output_row = output.loc[(output["Player"] == player_name) & (output["Team"] == team)].copy()
        player_dob = orig_df.loc[(orig_df[fb.PLAYER.N] == player_name) & (orig_df[fb.TEAM.N] == team), "dob"].iloc[0]
        if player_dob != pd.Timestamp(1900, 1, 1):
            output_row["Age"] = int((max(orig_df[fb.DATE.N]) - player_dob).days / 365)
        else:
            output_row["Age"] = None
        output_row["image_team"] = output_row["Team"]
        output_row["image_league"] = output_row["Competition"]
        output_row["Team"] = self._get_decorated_team_name(output_row["Team"].iloc[0], gender)
        output_row["Competition"] = self._get_decorated_league_name(output_row["Competition"].iloc[0])
        output_row["All Competitions"] = ",".join(orig_df[fb.COMPETITION.N].unique().tolist())
        return output_row


class MidfieldPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return MFTemplate

    @classmethod
    def get_name(cls) -> str:
        return "CMPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["CM", "DM"]


class CBPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return CBTemplate

    @classmethod
    def get_name(cls) -> str:
        return "CBPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["CB"]


class FBPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return FBTemplate

    @classmethod
    def get_name(cls) -> str:
        return "FBPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["LWB", "RWB", "WB", "RB", "LB"]


class FWPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return AttackerTemplate

    @classmethod
    def get_name(cls) -> str:
        return "FWPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["FW"]


class AMPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return AttackerTemplate

    @classmethod
    def get_name(cls) -> str:
        return "AMPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["AM", "LW", "RW", "RM", "LM"]


class AttackerCombinedPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return AttackerTemplate

    @classmethod
    def get_name(cls) -> str:
        return "ATTPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["AM", "LW", "RW", "RM", "LM", "FW"]


class GKPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return GoalkeeperTemplate

    @classmethod
    def get_name(cls) -> str:
        return "GKPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["GK"]


class TargetmanDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return TargetmanTemplate

    @classmethod
    def get_name(cls) -> str:
        return "TargetmanPizza"

    def get_comparison_positions(self) -> List[str]:
        return ["FW"]


class TeamPizzaDataSource(DataSource):
    def _aggregate_by_team(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = set(df.columns) - set(
            [
                fb.TEAM.N,
                fb.COMPETITION.N,
                fb.YEAR.N,
                "match_id",
                "gender",
                fb.OPPONENT.N,
            ]
        )
        agg_dict = {col: "sum" for col in cols}
        agg_dict.update({"match_id": "nunique"})
        df = df.groupby([fb.TEAM.N, fb.COMPETITION.N, fb.YEAR.N, "gender"]).agg(agg_dict).reset_index()
        df = df.rename(columns={"match_id": "matches"})
        return df

    def _aggregate_shot_data(self, df_shots: pd.DataFrame) -> pd.DataFrame:
        cols_to_sum = ["live_xg", "setpiece_xg", "big_chance"]
        team_df = df_shots.groupby([fb.TEAM.N, fb.COMPETITION.N]).agg({c: "sum" for c in cols_to_sum}).reset_index()
        opposition_df = (
            df_shots.groupby([fb.OPPONENT.N, fb.COMPETITION.N]).agg({c: "sum" for c in cols_to_sum}).reset_index()
        )
        combined_df = pd.merge(
            team_df,
            opposition_df,
            how="inner",
            left_on=[fb.TEAM.N, fb.COMPETITION.N],
            right_on=[fb.OPPONENT.N, fb.COMPETITION.N],
            suffixes=("_team", "_opp"),
        ).reset_index()
        return combined_df

    def _aggregate_by_opponent(self, df: pd.DataFrame) -> pd.DataFrame:
        cols = set(df.columns) - set(
            [
                fb.OPPONENT.N,
                fb.COMPETITION.N,
                fb.YEAR.N,
                "match_id",
                "gender",
                fb.TEAM.N,
            ]
        )
        agg_dict = {col: "sum" for col in cols}
        df = df.groupby([fb.OPPONENT.N, fb.COMPETITION.N, fb.YEAR.N, "gender"]).agg(agg_dict).reset_index()
        return df

    @classmethod
    def get_name(cls) -> str:
        return "TeamPizza"

    def get_template(self) -> List[TemplateAttribute]:
        return TeamTemplate

    def _specific_position_impl(self, data: pd.DataFrame) -> dict:
        data_rank = {
            attrib.name: attrib.calculation(data).rank(pct=True, method="min", ascending=attrib.ascending_rank)
            # attrib.name: attrib.calculation(data)
            for attrib in self.get_template()
        }
        data_value = {
            f"{attrib.name}__value": attrib.calculation(data).apply(lambda x: f"{x:.{attrib.sig_figs}f}")
            for attrib in self.get_template()
        }

        return {**data_rank, **data_value}

    def get_data_dict(self, data):
        specific_data = self._specific_position_impl(data)
        data_dict = {
            "Team": data[fb.TEAM.N].tolist(),
            "Competition": data[fb.COMPETITION.N].tolist(),
            "Season": data[fb.YEAR.N].tolist(),
        }
        data_dict.update(specific_data)
        return data_dict

    def _get_shots_data(self, leagues, season):
        leagues_str = "'" + "','".join(leagues) + "'"
        conn = Connection("M0neyMa$e")
        data = conn.query(
            f"""
            SELECT 
            T1.squad, 
            T1.xg,
            T1.psxg,
            T1.outcome,
            T1.notes,
            T1.sca_1_event,
            T1.comp,
            T1.season,
            T2.opponent FROM fbref_shots T1
            LEFT JOIN 
            (
                SELECT DISTINCT(match_id),squad,opponent FROM fbref 
                WHERE season={season} and comp IN ({leagues_str})
            ) T2
            ON 
            T1.match_id=T2.match_id 
            AND T1.squad=T2.squad
            WHERE T1.season={season} AND T1.comp IN ({leagues_str})
            """
        )
        data["is_open_play"] = data.apply(
            lambda r: (r["notes"] != "penalty") & (r["sca_1_event"] not in ["Pass (Dead)", "Fouled"]),
            axis=1,
        )
        data["is_penalty"] = data["notes"].apply(lambda x: x == "penalty")
        data["live_xg"] = data["xg"] * data["is_open_play"].astype(int)
        data["setpiece_xg"] = data["xg"] * (~data["is_open_play"]).astype(int) * (~data["is_penalty"]).astype(int)
        data["big_chance"] = (data["xg"] > 0.3).astype(int)
        return data

    def impl_get_data(self, season: int, leagues: List[str], team: str) -> pd.DataFrame:
        conn = Connection("M0neyMa$e")

        template = self.get_template()
        all_template_columns = [attr.columns_used for attr in template]
        all_template_columns = [item for sublist in all_template_columns for item in sublist]
        all_template_columns = list(set(all_template_columns))
        all_columns = [
            fb.TEAM.N,
            fb.OPPONENT.N,
            fb.COMPETITION.N,
            fb.YEAR.N,
            "gender",
            "match_id",
        ] + all_template_columns
        data = conn.query(
            f"""
            SELECT  `{'`,`'.join(all_columns)}`  FROM fbref WHERE comp in ({','.join([f"'{league}'" for league in leagues])}) AND season = {season}
        """
        )
        shot_data = self._get_shots_data(leagues, season)
        combined_shot_data = self._aggregate_shot_data(shot_data)
        data_agg_by_team = self._aggregate_by_team(data)
        data_agg_by_opponent = self._aggregate_by_opponent(data)
        data_combined = data_agg_by_team.merge(
            data_agg_by_opponent,
            left_on=[fb.TEAM.N, fb.COMPETITION.N, fb.YEAR.N, "gender"],
            right_on=[fb.OPPONENT.N, fb.COMPETITION.N, fb.YEAR.N, "gender"],
            how="outer",
            suffixes=("_team", "_opp"),
        ).reset_index()
        data_combined = data_combined.merge(
            combined_shot_data,
            left_on=[fb.TEAM.N, fb.COMPETITION.N],
            right_on=[fb.TEAM.N, fb.COMPETITION.N],
            how="outer",
        )
        data_combined = data_combined.fillna(0)
        data_dict = self.get_data_dict(data_combined)
        output = pd.DataFrame(data_dict)
        decorated_league_names = get_multiple_decorated_league_names_from_fb_names(leagues)
        output = output[output["Team"] == team]
        output["All Leagues"] = ", ".join(decorated_league_names.values())
        output["Decorated League"] = output["Competition"].apply(lambda x: decorated_league_names[x])
        output["Decorated Team"] = get_decorated_team_name_from_fb_name(team, output["Competition"].tolist()[0])
        return output
