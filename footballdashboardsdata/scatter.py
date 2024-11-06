from typing import List, Tuple
from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection
import pandas as pd
import datetime as dt
from sklearn.preprocessing import MinMaxScaler
import json


COMPUTED_STATS = [
    (
        "aerial_pct",
        "Contested Header Win Pct",
        "SUM(aerials_won)/(SUM(aerials_won)+SUM(aerials_lost))",
        False,
    ),
    ("tackles_won_pct", "Tackle Win Pct", "SUM(tackles_won)/SUM(tackles)", False),
    (
        "def_ground_duels_won_pct",
        "Defensive Ground Duel Win Pct",
        "SUM(tackles_vs_dribbles_won)/SUM(tackles_vs_dribbles)",
        False,
    ),
    (
        "psxg_minus_goals_conceded",
        "PSxG - Goals Conceded",
        "SUM(psxg_gk) - SUM(goals_conceded)",
        True,
    ),
    (
        "conversion_rate",
        "Shot Conversion Rate",
        "SUM(goals)/SUM(shots_total)",
        False,
    ),
    (
        "shots_on_target_pct",
        "Shots on Target Pct",
        "SUM(shots_on_target)/SUM(shots_total)",
        False,
    ),
    (
        "shots_per_box_touch",
        "Shots per Touches in Pen Area",
        "SUM(shots_total)/SUM(touches_att_pen_area)",
        False,
    ),
    ("x_goal_contributions", "NPxG + xA", "SUM(npxg)+SUM(xag)", True),
]


def add_years(original_date, years):
    try:
        # Calculate the target year
        target_year = original_date.year + years

        # Try to create a new date with the target year
        new_date = original_date.replace(year=target_year)
    except ValueError:
        # Handle cases where the new date is invalid (e.g., February 29th on a non-leap year)
        new_date = original_date + (
            dt.date(target_year, 3, 1) - dt.date(original_date.year, 3, 1)
        )

    return new_date


class ScatterDataSource(DataSource):

    COL_RENAME_DICT = {
        "enriched_position": "Position",
        "xag": "xAG",
        "npxg": "NPxG",
        "xa": "xA",
        "npxg_xa": "NPxG+xA",
        "xg": "xG",
        "comp": "League",
        "squad": "Team",
        "pens_made": "Penalties Scored",
        "pens_att": "Penalties Attempted",
        "shots_total": "Total Shots",
        "cards_yellow": "Yellow Cards",
        "cards_red": "Red Cards",
        "sca": "Shot Creating Actions",
        "gca": "Goal Creating Actions",
        "dribbles_completed": "Completed Take Ons",
        "dribbles": "Take Ons",
        "passes_completed": "Completed Passes",
        "passes_total_distance": "Total Pass Distance",
        "passes_progressive_distance": "Progressive Pass Distance",
        "passes_completed_short": "Completed Short Passes",
        "passes_completed_medium": "Completed Medium Passes",
        "passes_completed_long": "Completed Long Passes",
        "passes_short": "Short Passes",
        "passes_medium": "Medium Passes",
        "passes_long": "Long Passes",
        "passes_dead": "Dead Ball Passes",
        "passes_live": "Open Play Passes",
        "passes_free_kicks": "Free Kick Passes",
        "passes_switches": "Switches",
        "pass types_ti": "Throw Ins",
        "passes_offsides": "Offside Passes",
        "passes_blocked": "Attempted Passes Blocked",
        "tackles_def_3rd": "Defensive Third Tackles",
        "tackles_mid_3rd": "Midfield Third Tackles",
        "tackles_att_3rd": "Attacking Third Tackles",
        "tackles_vs_dribbles_won": "Tackles vs Dribbles Won",
        "tackles_vs_dribbles": "Tackles vs Dribbles Attempted",
        "blocked_passes": "Blocked Passes (Defensive)",
        "blocked_shots": "Shots Blocked(Defensive)",
        "touches_def_pen_area": "Touches in Defensive Penalty Area",
        "touches_def_3rd": "Touches in Defensive Third",
        "touches_mid_3rd": "Touches in Midfield Third",
        "touches_att_3rd": "Touches in Attacking Third",
        "touches_att_pen_area": "Touches in Attacking Penalty Area",
        "touches_live_ball": "Touches (Open Play)",
        "cards_yellow_red": "Total Cards Received",
        "offsides": "Caught Offside",
        "pens_won": "Penalties Won",
        "pens_conceded": "Penalties Conceded",
        "aerials_won": "Contested Headers Won",
        "aerials_lost": "Contested Headers Lost",
        "shots_on_target_against": "Shots on Target Faced",
        "psxg_gk": "Post-Shot Expected Goals (GK)",
        "passes_launched_completed": "Passes Longer than 40 Yards Completed",
        "passes_launched": "Passes Longer than 40 Yards Attempted",
        "passes_length_avg_gk": "Average Pass Length (GK)",
        "goal_kick_length_avg": "Average Goal Kick Length",
        "crosses_gk": "Crosses Faced (GK)",
        "crosses_stopped_gk": "Crosses Collected (GK)",
        "sweeper_action_avg_distance": "Average Sweeper Keeper Distance",
        "passes_launched_total": "Total Long Passes (GK)",
        "sca_passes_live": "Open Play Pass Shot Creating Actions",
        "sca_passes_dead": "Dead Ball Pass Shot Creating Actions",
        "sca_dribbles": "Take On Shot Creating Actions",
        "sca_shots": "Shot Creating Actions from a Shot",
        "sca_fouled": "Fouls Drawn Shot Creating Actions",
        "sca_defense": "Defensive Shot Creating Actions",
        "gca_passes_live": "Open Play Pass Goal Creating Actions",
        "gca_passes_dead": "Dead Ball Pass Goal Creating Actions",
        "gca_dribbles": "Take On Goal Creating Actions",
        "gca_shots": "Goal Creating Actions from a Shot",
        "gca_fouled": "Fouls Drawn Goal Creating Actions",
        "gca_defense": "Defensive Goal Creating Actions",
        "shots_saved": "Saved Shots",
        "shots_blocked": "Blocked Shots",
        "shots_offtarget": "Off Target Shots",
        "shots_woodwork": "Shots Hitting the Woodwork",
        "shots_leftfoot": "Left Footed Shots",
        "shots_rightfoot": "Right Footed Shots",
        "shots_head": "Headed Shots",
        "shots_bodypart_other": "Shots from Body Parts Other than Feet or Head",
        "goals_leftfoot": "Left Footed Goals",
        "goals_rightfoot": "Right Footed Goals",
        "goals_head": "Headed Goals",
        "goals_bodypart_other": "Goals from Body Parts Other than Feet or Head",
    }

    MOST_COMMON_AGG_COLS = ["enriched_position"]
    CATEGORICAL_COLS = ["comp", "team"]
    POSITION_COLS = ["enriched_position"]

    @classmethod
    def get_name(cls):
        return "ScatterDataSource"

    def _format_label(self, label: str, normalize_per_90) -> str:
        try:
            label_n = next(t[1] for t in COMPUTED_STATS if t[0] == label)
        except:
            if label in self.COL_RENAME_DICT:
                label_n = self.COL_RENAME_DICT[label]
            else:
                label_n = label.replace("_", " ").title()
        if (
            normalize_per_90
            and label != "minutes"
            and label not in self.CATEGORICAL_COLS
            and label not in self.MOST_COMMON_AGG_COLS
            and label not in [v[0] for v in COMPUTED_STATS if v[3] == False]
        ):
            label_n += " P90"
        return label_n

    def _construct_player_query_string(
        self,
        leagues: List[str],
        season: int,
        teams: List[str],
        x_axis: str,
        y_axis: str,
        size_axis: str,
        color_axis: str,
        position_filter: List[str],
        dob_values_filter: Tuple[int, int],
    ) -> str:
        try:
            x_axis = next(t[2] for t in COMPUTED_STATS if t[0] == x_axis)
        except:
            x_axis = f"SUM({x_axis})"
        try:
            y_axis = next(t[2] for t in COMPUTED_STATS if t[0] == y_axis)
        except:
            y_axis = f"SUM({y_axis})"

        league_string = ", ".join([f"'{league}'" for league in leagues])
        query_string = f"""
        SELECT player_id, player, comp, squad,season,
        SUM(minutes) AS minutes,
        {x_axis} AS x_axis,
        {y_axis} AS y_axis
        """
        if size_axis:
            query_string += f",SUM({size_axis}) AS size_axis"
        if color_axis:
            if color_axis not in self.MOST_COMMON_AGG_COLS:
                if color_axis in self.CATEGORICAL_COLS:
                    query_string += f",{color_axis} AS color_axis"
                else:

                    color_axis = next(
                        (t[2] for t in COMPUTED_STATS if t[0] == color_axis),
                        f"SUM({color_axis})",
                    )
                    query_string += f",{color_axis} AS color_axis"
        query_string += f"""
        FROM fbref AS t1
        WHERE season = {season}
        AND dob BETWEEN '{dob_values_filter[0]}' AND '{dob_values_filter[1]}'
        AND comp IN ({league_string})
        """
        if position_filter:
            position_string = ", ".join(
                [f"'{position}'" for position in position_filter]
            )
            query_string += f"AND enriched_position IN ({position_string})"
        if teams:
            team_string = ", ".join([f"'{team}'" for team in teams])
            query_string += f"AND squad IN ({team_string})"
        query_string += """
        GROUP BY player_id, player, comp, squad
        """

        if color_axis and color_axis in self.MOST_COMMON_AGG_COLS:
            orig_query_string = query_string
            query_string = f"""
                SELECT T1.player_id, T1.season, T1.player, T1.comp, T1.squad, T1.minutes, T1.x_axis, T1.y_axis, 
            """
            if size_axis:
                query_string += f"T1.size_axis, "
            query_string += f"""T2.{color_axis} AS color_axis
                FROM ({orig_query_string}) AS T1
                JOIN (
                    WITH RankedCatgroups AS (
                        SELECT 
                            player_id,
                            player,
                            squad,
                            comp,
                    
                            {color_axis},
                            ROW_NUMBER() OVER (PARTITION BY player_id, player,squad, comp ORDER BY COUNT(*) DESC) AS rn
                        FROM fbref
                        WHERE season={season}
                        AND dob BETWEEN '{dob_values_filter[0]}' AND '{dob_values_filter[1]}'
                        AND comp in ({league_string})
                """
            if teams:
                query_string += f"AND squad IN ({team_string})"
            if position_filter:
                query_string += f"AND enriched_position IN ({position_string})"

            query_string += f"""
                    GROUP BY player_id, player, squad, comp, {color_axis}
                )
                SELECT 
                    player_id,
                    player,
                    squad,
                    comp,
                    {color_axis}
                FROM RankedCatgroups
                WHERE rn = 1
                )  T2
                ON T1.player_id = T2.player_id
                AND T1.player = T2.player
                AND T1.squad = T2.squad
                AND T1.comp = T2.comp
            """
        print(query_string)
        return query_string

    def impl_get_data(
        self,
        leagues: List[str],
        season: int,
        team_aggregation: bool,
        x_axis: str,
        y_axis: str,
        teams: List[str] = None,
        item_highlights: List[str] = None,
        team_highlights: List[str] = None,
        size_axis: str = None,
        color_axis: str = None,
        position_filter: List[str] = None,
        minutes_filter: int = 0,
        normalize_per_90: bool = True,
        age_filter: Tuple[int, int] = None,
        value_highlights: Tuple[Tuple[float, float], Tuple[float, float]] = None,
    ):
        _position_filter = position_filter.copy() or []
        if "LB" in _position_filter:
            _position_filter.append("LWB")
        if "RB" in _position_filter:
            _position_filter.append("RWB")
        if "LW" in _position_filter:
            _position_filter.append("LM")
        if "RW" in _position_filter:
            _position_filter.append("RM")

        item_highlights = item_highlights or []
        team_highlights = team_highlights or []
        value_highlights = value_highlights or ((0, 1), (0, 1))
        age_filter = age_filter or (0, 100)
        dob_values = (
            add_years(dt.datetime.now(), -age_filter[1]).strftime("%Y-%m-%d"),
            add_years(dt.datetime.now(), -age_filter[0]).strftime("%Y-%m-%d"),
        )
        if not team_aggregation:
            data = self._get_player_data(
                leagues=leagues,
                season=season,
                teams=teams,
                x_axis=x_axis,
                y_axis=y_axis,
                size_axis=size_axis,
                color_axis=color_axis,
                position_filter=_position_filter,
                minutes_filter=minutes_filter,
                normalize_per_90=normalize_per_90,
                dob_values_filter=dob_values,
            )

        if not team_aggregation:
            data["annotate"] = data["player"].apply(lambda x: x in item_highlights)
            data["annotate"] = data["annotate"] | data["squad"].apply(
                lambda x: x in team_highlights
            )
        else:
            data["annotate"] = data["squad"].apply(lambda x: x in item_highlights)

        value_scaler = MinMaxScaler().fit_transform(data[["x_axis", "y_axis"]])
        data["annotate"] = data["annotate"] | (
            (value_scaler[:, 0] < value_highlights[0][0])
            | (value_scaler[:, 0] > value_highlights[0][1])
            | (value_scaler[:, 1] < value_highlights[1][0])
            | (value_scaler[:, 1] > value_highlights[1][1])
        )

        data["x_axis_name"] = self._format_label(x_axis, normalize_per_90)
        data["y_axis_name"] = self._format_label(y_axis, normalize_per_90)
        if size_axis:
            data["size_axis_name"] = self._format_label(size_axis, normalize_per_90)
        if color_axis:
            data["color_axis_name"] = self._format_label(color_axis, normalize_per_90)

        if color_axis in self.POSITION_COLS:
            data["color_axis"] = data["color_axis"].apply(
                lambda x: {"LWB": "LB", "RWB": "RB", "LM": "LW", "RM": "RW"}.get(x, x)
            )
        data = data.dropna()
        data["filter_data"] = json.dumps(
            {
                "leagues": leagues,
                "season": season,
                "teams": teams,
                "position_filter": position_filter,
                "minutes_filter": minutes_filter,
                "normalize_per_90": normalize_per_90,
                "age_filter": age_filter,
            }
        )
        return data

    def _fix_types(self, data: pd.DataFrame, color_axis: str) -> pd.DataFrame:
        data["x_axis"] = data["x_axis"].astype(float)
        data["y_axis"] = data["y_axis"].astype(float)
        data["minutes"] = data["minutes"].astype(float)
        if "size_axis" in data.columns:
            data["size_axis"] = data["size_axis"].astype(float)
        if "color_axis" in data.columns and (
            color_axis not in self.CATEGORICAL_COLS
            and color_axis not in self.MOST_COMMON_AGG_COLS
        ):
            data["color_axis"] = data["color_axis"].astype(float)
        return data

    def _get_player_data(
        self,
        leagues: List[str],
        season: int,
        teams: List[str],
        x_axis: str,
        y_axis: str,
        size_axis: str,
        color_axis: str,
        position_filter: List[str],
        normalize_per_90: bool,
        minutes_filter: int,
        dob_values_filter: Tuple[int, int],
    ) -> pd.DataFrame:

        query_string = self._construct_player_query_string(
            leagues=leagues,
            season=season,
            teams=teams,
            x_axis=x_axis,
            y_axis=y_axis,
            size_axis=size_axis,
            color_axis=color_axis,
            position_filter=position_filter,
            dob_values_filter=dob_values_filter,
        )
        raw_data = Connection("M0neyMa$e").query(query_string)
        # raw_data = self._fix_types(raw_data, color_axis)
        raw_data = raw_data[raw_data["minutes"] > minutes_filter]

        if normalize_per_90:
            if x_axis != "minutes" and x_axis not in [
                v[0] for v in COMPUTED_STATS if v[3] == False
            ]:
                raw_data["x_axis"] = raw_data["x_axis"] / raw_data["minutes"] * 90
            if y_axis != "minutes" and y_axis not in [
                v[0] for v in COMPUTED_STATS if v[3] == False
            ]:
                raw_data["y_axis"] = raw_data["y_axis"] / raw_data["minutes"] * 90
            if size_axis and size_axis != "minutes":
                raw_data["size_axis"] = raw_data["size_axis"] / raw_data["minutes"] * 90
            if (
                color_axis
                and color_axis != "minutes"
                and (
                    color_axis not in self.CATEGORICAL_COLS
                    and color_axis not in self.MOST_COMMON_AGG_COLS
                    and color_axis
                    not in [v[0] for v in COMPUTED_STATS if v[3] == False]
                )
            ):
                raw_data["color_axis"] = (
                    raw_data["color_axis"] / raw_data["minutes"] * 90
                )
        return raw_data
