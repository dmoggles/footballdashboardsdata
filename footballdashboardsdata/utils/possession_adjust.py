import pandas as pd

def aggregate_to_match_id(data:pd.DataFrame)->pd.DataFrame:
  return data.groupby(['match_id','comp','season']).agg({'touches':'sum'}).reset_index()

def aggregate_to_league_season_team(data:pd.DataFrame)->pd.DataFrame:
  return data.groupby(['comp','squad','season']).agg({'touches':'sum','match_id':'nunique'}).reset_index()

def aggregate_to_league_season_opponent(data:pd.DataFrame)->pd.DataFrame:
  return data.groupby(['comp','opponent','season']).agg({'touches':'sum','match_id':'nunique'}).reset_index()


def compute_league_average(data:pd.DataFrame)->pd.DataFrame:
  return data.pipe(
      aggregate_to_match_id
  ).groupby(['comp','season']).agg({'touches':'mean'}).reset_index()

def compute_team_average(data:pd.DataFrame)->pd.DataFrame:
  data = data.pipe(aggregate_to_league_season_team)
  data['touches_per_match'] = data['touches']/data['match_id']
  return data[['squad','comp','season','touches_per_match']]
def compute_opponent_average(data:pd.DataFrame)->pd.DataFrame:
  data = data.pipe(aggregate_to_league_season_opponent)
  data['touches_per_match'] = data['touches']/data['match_id']
  return data[['opponent','comp','season','touches_per_match']]

def total_touches_per_match(data:pd.DataFrame)->pd.DataFrame:
  team_averages = data.pipe(compute_team_average)
  opponent_averages = data.pipe(compute_opponent_average)
  combined = pd.merge(left=team_averages, right=opponent_averages, left_on=['squad','comp','season'],right_on=['opponent','comp','season'], suffixes=('_team','_opponent'))

  combined['per_game_touches']=combined['touches_per_match_team']+combined['touches_per_match_opponent']
  return combined[['comp','squad','season','per_game_touches']]


def total_possession_factor(data:pd.DataFrame)->pd.DataFrame:
  ttpm = data.pipe(total_touches_per_match)
  league_averages = data.pipe(compute_league_average)
  combined = pd.merge(left=ttpm, right=league_averages, on=['comp','season'])
  combined['total_possession_factor']=combined['touches']/combined['per_game_touches']

  return combined[['comp','squad','season','total_possession_factor']]

def in_possession_factor(data:pd.DataFrame)->pd.DataFrame:
  team_averages = data.pipe(compute_team_average)
  ttpm = data.pipe(total_touches_per_match)
  combined = pd.merge(
      left = team_averages,
      right = ttpm,
      left_on = ['comp','squad','season'],
      right_on  = ['comp','squad','season']

  )
  combined['in_possession_factor'] = 0.5 / (combined['touches_per_match']/combined['per_game_touches'])
  return combined[['comp','squad','season','in_possession_factor']]

def out_possession_factor(data:pd.DataFrame)->pd.DataFrame:
  opp_averages = data.pipe(compute_opponent_average)
  ttpm = data.pipe(total_touches_per_match)
  combined = pd.merge(
      left = opp_averages,
      right = ttpm,
      left_on = ['comp','opponent','season'],
      right_on  = ['comp','squad','season']

  )
  combined['out_possession_factor'] = 0.5 / (combined['touches_per_match']/combined['per_game_touches'])
  return combined[['comp','squad','season','out_possession_factor']]

def adj_possession_factors(data:pd.DataFrame)->pd.DataFrame:
  combined = pd.merge(
      left=data.pipe(total_possession_factor),
      right = data.pipe(in_possession_factor),
      on=['comp','squad','season']
  )
  combined = pd.merge(
      left=combined,
      right=data.pipe(out_possession_factor),
      on=['comp','squad','season']
  )
  combined['in_possession_factor']=combined['in_possession_factor']*combined['total_possession_factor']
  combined['out_of_possession_factor']=combined['out_possession_factor']*combined['total_possession_factor']
  return combined[['comp','squad','season','in_possession_factor','out_of_possession_factor']]
