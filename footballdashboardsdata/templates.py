from typing import Callable
import dataclasses
import pandas as pd
from footmav import fb
@dataclasses.dataclass
class TemplateAttribute:
    name: str
    calculation: Callable[[pd.DataFrame], pd.Series]
    ascending_rank:bool
    columns_used: list = dataclasses.field(default_factory=list)



MFTemplate=[
    TemplateAttribute(
        name='Passing %',
        calculation=lambda df: (df[fb.PASSES_COMPLETED.N]/df[fb.PASSES.N]),
        ascending_rank=True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N]
    ),
    TemplateAttribute(
        name='Key Passes',
        calculation=lambda df: df[fb.ASSISTED_SHOTS.N],
        ascending_rank=True,
        columns_used=[fb.ASSISTED_SHOTS.N]
    ),
    TemplateAttribute(
        name='Throughballs',
        calculation=lambda df: df[fb.THROUGH_BALLS.N],
        ascending_rank=True,
        columns_used=[fb.THROUGH_BALLS.N]
    ),
    TemplateAttribute(
        name='Scoring Contributions',
        calculation=lambda df: (df[fb.NON_PENALTY_GOALS.N] + df[fb.ASSISTS.N]),
        ascending_rank=True,
        columns_used=[fb.GOALS.N, fb.ASSISTS.N, fb.PENS_MADE.N]
    ),
    TemplateAttribute(
        'Successful Dribbles',
        lambda df: df[fb.DRIBBLES_COMPLETED.N],
        True,
        columns_used=[fb.DRIBBLES_COMPLETED.N]
    ),
    TemplateAttribute(
        'Dispossesses',
        lambda df: df[fb.DISPOSSESSED.N],
        False,
        columns_used=[fb.DISPOSSESSED.N]
    ),
    TemplateAttribute(
        'Fouls',
        lambda df: df[fb.FOULS.N],
        False,
        columns_used=[fb.FOULS.N]
    ),
    TemplateAttribute(
        'PAdj Dribbled Past',
        lambda df: df[fb.DRIBBLED_PAST.N]*df['out_of_possession_factor'],
        False,
        columns_used=[fb.DRIBBLED_PAST.N]
    ),
    TemplateAttribute(
        'PAdj Tackles',
        lambda df: df[fb.TACKLES.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.TACKLES.N]
    ),
    TemplateAttribute(
        'PAdj Interceptions',
        lambda df: df[fb.INTERCEPTIONS.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.INTERCEPTIONS.N]
    ),
    TemplateAttribute(
        'Progressive Passes',
        lambda df: df[fb.PROGRESSIVE_PASSES.N],
        True,
        columns_used=[fb.PROGRESSIVE_PASSES.N]
    ),
]

CBTemplate=[
    TemplateAttribute(
        name='Passing %',
        calculation=lambda df: (df[fb.PASSES_COMPLETED.N]/df[fb.PASSES.N]),
        ascending_rank=True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N]
    ),
     TemplateAttribute(
        'PAdj Dribbled Past',
        lambda df: df[fb.DRIBBLED_PAST.N]*df['out_of_possession_factor'],
        False,
        columns_used=[fb.DRIBBLED_PAST.N]
    ),
    TemplateAttribute(
        'PAdj Tackles',
        lambda df: df[fb.TACKLES.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.TACKLES.N]
    ),
    TemplateAttribute(
        'PAdj Interceptions',
        lambda df: df[fb.INTERCEPTIONS.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.INTERCEPTIONS.N]
    ),
    TemplateAttribute(
        'PAdj Blocks',
        lambda df: df[fb.BLOCKS.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.BLOCKS.N]
    ),
    TemplateAttribute(
        'PAdj Clearances',
        lambda df: df[fb.CLEARANCES.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.CLEARANCES.N]
    ),
    TemplateAttribute(
        'Fouls',
        lambda df: df[fb.FOULS.N],
        False,
        columns_used=[fb.FOULS.N]
    ),
    TemplateAttribute(
        'Headers Won',
        lambda df: df[fb.AERIALS_WON.N],
        True,
        columns_used=[fb.AERIALS_WON.N]
    ),
    TemplateAttribute(
        'Headers Won %',
        lambda df: 100*df[fb.AERIALS_WON.N]/(df[fb.AERIALS_WON.N]+df[fb.AERIALS_LOST.N]),
        True,
        columns_used=[fb.AERIALS_WON.N, fb.AERIALS_LOST.N]
    ),
    TemplateAttribute(
        'Progressive Passes',
        lambda df: df[fb.PROGRESSIVE_PASSES.N],
        True,
        columns_used=[fb.PROGRESSIVE_PASSES.N]
    ),
    TemplateAttribute(
        'Progressive Passes %',
        lambda df: 100*df[fb.PROGRESSIVE_PASSES.N]/df[fb.PASSES.N],
        True,
        columns_used=[fb.PROGRESSIVE_PASSES.N, fb.PASSES.N]
    ),
]
FBTemplate=[
    TemplateAttribute(
        name='Passing %',
        calculation=lambda df: (df[fb.PASSES_COMPLETED.N]/df[fb.PASSES.N]),
        ascending_rank=True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N]
    ),
    TemplateAttribute(
        'PAdj Tackles',
        lambda df: df[fb.TACKLES.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.TACKLES.N]
    ),
    TemplateAttribute(
        'PAdj Interceptions',
        lambda df: df[fb.INTERCEPTIONS.N]*df['out_of_possession_factor'],
        True,
        columns_used=[fb.INTERCEPTIONS.N]
    ),
    TemplateAttribute(
        name='Key Passes',
        calculation=lambda df: df[fb.ASSISTED_SHOTS.N],
        ascending_rank=True,
        columns_used=[fb.ASSISTED_SHOTS.N]
    ),
    TemplateAttribute(
        name='Completed Crosses',
        calculation=lambda df: df[fb.CROSSES_INTO_PENALTY_AREA.N],
        ascending_rank=True,
        columns_used=[fb.CROSSES_INTO_PENALTY_AREA.N]
    ),
    TemplateAttribute(
        name='Crosses %',
        calculation=lambda df: 100*df[fb.CROSSES_INTO_PENALTY_AREA.N]/df[fb.CROSSES.N],
        ascending_rank=True,
        columns_used=[fb.CROSSES_INTO_PENALTY_AREA.N, fb.CROSSES.N]
    ),
    TemplateAttribute(
        'Successful Dribbles',
        lambda df: df[fb.DRIBBLES_COMPLETED.N],
        True,
        columns_used=[fb.DRIBBLES_COMPLETED.N]
    ),
    TemplateAttribute(
        'Dispossesses',
        lambda df: df[fb.DISPOSSESSED.N],
        False,
        columns_used=[fb.DISPOSSESSED.N]
    ),
    TemplateAttribute(
        name='Scoring Contributions',
        calculation=lambda df: (df[fb.NON_PENALTY_GOALS.N] + df[fb.ASSISTS.N]),
        ascending_rank=True,
        columns_used=[fb.GOALS.N, fb.ASSISTS.N, fb.PENS_MADE.N]
    ),
    TemplateAttribute(
        'PAdj Dribbled Past',
        lambda df: df[fb.DRIBBLED_PAST.N]*df['out_of_possession_factor'],
        False,
        columns_used=[fb.DRIBBLED_PAST.N]
    ),
    TemplateAttribute(
        'Fouls',
        lambda df: df[fb.FOULS.N],
        False,
        columns_used=[fb.FOULS.N]
    ),
]
AttackerTemplate=[
    TemplateAttribute(
        'Non-Penalty Goals',
        lambda df: df[fb.NON_PENALTY_GOALS.N],
        True,
        columns_used=[fb.GOALS.N, fb.PENS_MADE.N]
    ),
    TemplateAttribute(
        'Shots',
        lambda df: df[fb.SHOTS_TOTAL.N],
        True,
        columns_used=[fb.SHOTS_TOTAL.N]
    ),
    TemplateAttribute(
        'Shooting %',
        lambda df: 100*df[fb.NON_PENALTY_GOALS.N]/df[fb.SHOTS_TOTAL.N],
        True,
        columns_used=[fb.GOALS.N,  fb.PENS_MADE.N, fb.SHOTS_TOTAL.N]
    ),
    TemplateAttribute(
        'Passing %',
        lambda df: (100*df[fb.PASSES_COMPLETED.N]/df[fb.PASSES.N]),
        True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N]
    ),
    TemplateAttribute(
        'Assists',
        lambda df: df[fb.ASSISTS.N],
        True,
        columns_used=[fb.ASSISTS.N]
    ),
    TemplateAttribute(
        'xA',
        lambda df: df[fb.XA.N],
        True,
        columns_used=[fb.XA.N]
    ),
    TemplateAttribute(
        'Through Balls',
        lambda df: df[fb.THROUGH_BALLS.N],
        True,
        columns_used=[fb.THROUGH_BALLS.N]
    ),
    TemplateAttribute(
        'Int+Tackles',
        lambda df: df[fb.INTERCEPTIONS.N]+df[fb.TACKLES.N],
        True,
        columns_used=[fb.INTERCEPTIONS.N, fb.TACKLES.N]
    ),
    TemplateAttribute(
        'Dispossessed',
        lambda df: df[fb.DISPOSSESSED.N],
        False,
        columns_used=[fb.DISPOSSESSED.N]
    ),
    TemplateAttribute(
        'Successful Dribbles',
        lambda df: df[fb.DRIBBLES_COMPLETED.N],
        True,
        columns_used=[fb.DRIBBLES_COMPLETED.N]
    ),
    TemplateAttribute(
        'NPxG',
        lambda df: df[fb.NPXG.N],
        True,
        columns_used=[fb.NPXG.N]
    ),
]

