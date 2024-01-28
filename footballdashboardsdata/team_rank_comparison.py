from typing import List, Tuple
import pandas as pd
from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection

METRICS = [
    ("performance", True, "Performance"),
    ("attack", True, "Attack"),
    ("defense", False, "Defense"),
    ("physical_duels", True, "Duels"),
    ("pressing", False, "Pressing"),
    ("set_piece", True, "Set Pieces"),
    ("box_efficiency", True, "Box Efficiency"),
    ("counterattack", True, "Counter"),
    ("possession", True, "Possession"),
]


def query_string(competitions: List[str], seasons: list[int]):
    comp_str = ",".join([f"'{c}'" for c in competitions])
    season_str = ",".join([f"{s}" for s in seasons])
    return f"""
 SELECT 
 MT.ws_team_name as team_name, 
 MT.decorated_name as decorated_team_name, 
 ML.decorated_name as decorated_league_name,
 D1.*, xpts/games as performance
 FROM (
   SELECT 
   season,
   competition,
   teamId,
   games,
   attack/games as attack,
   defense/games as defense,
   physical_duels/games as physical_duels,
   ppda_qualifying_passes/ppda_qualifying_defensive_actions as pressing,
   set_piece/games as set_piece,
   box_efficiency/games as box_efficiency,
   counterattack_shots/shots as counterattack,
   touch_for/(touch_for+touch_against) as possession
   
   FROM(


     SELECT 
     season,
     competition, 
     teamId,
     games,
     SUM(CASE WHEN metric_name='attack' THEN metric_value ELSE NULL END) AS attack,
     SUM(CASE WHEN metric_name='defense' THEN metric_value ELSE NULL END) AS defense,
     SUM(CASE WHEN metric_name='physical_duels' THEN metric_value ELSE NULL END) AS physical_duels,
     SUM(CASE WHEN metric_name='ppda_qualifying_defensive_actions' THEN metric_value ELSE NULL END) AS ppda_qualifying_defensive_actions,
     SUM(CASE WHEN metric_name='ppda_qualifying_passes' THEN metric_value ELSE NULL END) AS ppda_qualifying_passes,
     SUM(CASE WHEN metric_name='set_piece' THEN metric_value ELSE NULL END) AS set_piece,
     SUM(CASE WHEN metric_name='box_efficiency' THEN metric_value ELSE NULL END) AS box_efficiency,
     SUM(CASE WHEN metric_name='shots' THEN metric_value ELSE NULL END) AS shots,
     SUM(CASE WHEN metric_name='counterattack_shots' THEN metric_value ELSE NULL END) AS counterattack_shots,
     SUM(CASE WHEN metric_name='touch_for' THEN metric_value ELSE NULL END) AS touch_for,
     SUM(CASE WHEN metric_name='touch_against' THEN metric_value ELSE NULL END) AS touch_against
     FROM
     (
      SELECT season, competition, teamId, metric_name, count(1) as games, sum(metric_value) as metric_value
      FROM
      (
        SELECT season, competition, teamId, metric_name, matchId,T1.value as metric_value 
        FROM agg.team_aggregations T1
        LEFT JOIN agg.team_agg_metric_definitions T2
        ON T1.metricId=T2.id

        WHERE 
        season IN ({season_str})
        AND 
        competition in ({comp_str})
        AND metric_name IN (
          'attack',
          'defense',
          'physical_duels',
          'ppda_qualifying_defensive_actions',
          'ppda_qualifying_passes',
          'set_piece',
          'box_efficiency',
          'shots',
          'counterattack_shots',
          'touch_for',
          'touch_against'
          )
      ) TV
      GROUP BY season,competition,teamId,metric_name
    ) TV2
    GROUP BY season,competition,teamId,games
  ) TV3
) D1
INNER JOIN football_data.mclachbot_teams MT
ON D1.teamId=MT.ws_team_id
INNER JOIN football_data.mclachbot_leagues ML
ON D1.competition=ML.ws_league_name
JOIN (
  SELECT competition,season,team_id, sum(xpts) as xpts
  FROM derived.whoscored_xpts
  WHERE 
  season IN ({season_str})
        AND 
  competition in ({comp_str})
  GROUP BY competition,season, team_id
) AS XPTS
ON D1.season=XPTS.season AND D1.competition=XPTS.competition AND D1.teamId=XPTS.team_id

    """


LEAGUE_GROUPS = [
    ["epl", "la liga", "bundesliga", "seriea", "ligue1"],
    [
        "mls",
        "ligaportugal",
        "JupilerProLeague",
        "eredivisie",
        "championship",
        "Brasileirao",
        "ArgentinePrimera",
    ],
]


class TeamRankDataSource(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "TeamRankComparision"

    def impl_get_data(
        self, team_choices: List[Tuple[str, int, int]], select_similar_leagues: bool
    ) -> pd.DataFrame:
        conn = Connection("M0neyMa$e")
        competitions = [c[0] for c in team_choices]
        # find complimentary competitions in LEAGUE_GROUPS
        if select_similar_leagues:
            for c in competitions.copy():
                for lg in LEAGUE_GROUPS:
                    if c in lg:
                        competitions.extend(lg)
                        break
        seasons = [c[2] for c in team_choices]
        query = query_string(set(competitions), seasons)
        data = conn.query(query)
        selected_teams = []
        for tc in team_choices:
            if select_similar_leagues:
                comp_leagues = next(c for c in LEAGUE_GROUPS if tc[0] in c)
            else:
                comp_leagues = [tc[0]]

            comp_data = data.loc[
                (data["season"] == tc[2]) & (data["competition"].isin(comp_leagues))
            ].copy()

            for column_def in METRICS:
                comp_data[column_def[0]] = comp_data[column_def[0]].rank(
                    ascending=column_def[1], pct=True, method="min"
                )
            col_renames = {c[0]: c[2] for c in METRICS}
            comp_data = comp_data.rename(columns=col_renames)
            # team_choice_tuples = [(c[1], c[2]) for c in team_choices]
            selected_teams.append(comp_data.loc[comp_data["teamId"] == tc[1]])
        data_for_teams = pd.concat(selected_teams)
        column_order = [
            c for c in data_for_teams.columns if c not in [m[2] for m in METRICS]
        ] + [m[2] for m in METRICS]
        return data_for_teams[column_order]
