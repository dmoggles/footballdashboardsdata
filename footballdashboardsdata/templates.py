from typing import Callable
import dataclasses
import enum
import pandas as pd
from footmav import fb


from footmav.data_definitions.base import IntDataAttribute, RegisteredAttributeStore
from footmav.data_definitions.data_sources import DataSource as DataSourceEnum

if not "self_created_shots" in [a.N for a in RegisteredAttributeStore.get_registered_attributes()]:
    SELF_CREATED_SHOTS = IntDataAttribute(name="self_created_shots", source=DataSourceEnum.FBREF)


class PossessionAdjustment:
    NONE = 0
    OUT_OF_POSS = 1
    IN_POSS = 2


@dataclasses.dataclass
class TemplateAttribute:
    name: str
    calculation: Callable[[pd.DataFrame], pd.Series]
    ascending_rank: bool
    columns_used: list = dataclasses.field(default_factory=list)
    possession_adjust: PossessionAdjustment = PossessionAdjustment.NONE
    sig_figs: int = 1


ASSISTS = TemplateAttribute(
    "Assists",
    lambda df: df[fb.ASSISTS.N],
    True,
    columns_used=[fb.ASSISTS.N],
    sig_figs=2,
)
BLOCKS_PADJ = TemplateAttribute(
    "PAdj Blocks",
    lambda df: df[fb.BLOCKS.N],
    True,
    columns_used=[fb.BLOCKS.N],
    possession_adjust=PossessionAdjustment.OUT_OF_POSS,
)
CLEARANCES_PADJ = TemplateAttribute(
    "PAdj Clearances",
    lambda df: df[fb.CLEARANCES.N],
    True,
    columns_used=[fb.CLEARANCES.N],
    possession_adjust=PossessionAdjustment.OUT_OF_POSS,
)
CROSSES_COMPLETED = TemplateAttribute(
    name="Completed Crosses",
    calculation=lambda df: df[fb.CROSSES_INTO_PENALTY_AREA.N],
    ascending_rank=True,
    columns_used=[fb.CROSSES_INTO_PENALTY_AREA.N],
    sig_figs=2,
)
CROSSES_PCT = TemplateAttribute(
    name="Crosses %",
    calculation=lambda df: (100 * df[fb.CROSSES_INTO_PENALTY_AREA.N] / df[fb.CROSSES.N]).fillna(0),
    ascending_rank=True,
    columns_used=[fb.CROSSES_INTO_PENALTY_AREA.N, fb.CROSSES.N],
)
DISPOSSESSED = TemplateAttribute(
    "Dispossessed",
    lambda df: df[fb.DISPOSSESSED.N],
    False,
    columns_used=[fb.DISPOSSESSED.N],
)
FOULS = TemplateAttribute("Fouls", lambda df: df[fb.FOULS.N], False, columns_used=[fb.FOULS.N])
HEADERS_WON = TemplateAttribute(
    "Headers Won",
    lambda df: df[fb.AERIALS_WON.N],
    True,
    columns_used=[fb.AERIALS_WON.N],
)
HEADERS_WON_PCT = TemplateAttribute(
    "Headers Won %",
    lambda df: (100 * df[fb.AERIALS_WON.N] / (df[fb.AERIALS_WON.N] + df[fb.AERIALS_LOST.N])).fillna(0),
    True,
    columns_used=[fb.AERIALS_WON.N, fb.AERIALS_LOST.N],
)
INTERCEPTIONS_PADJ = TemplateAttribute(
    "PAdj Interceptions",
    lambda df: df[fb.INTERCEPTIONS.N],
    True,
    columns_used=[fb.INTERCEPTIONS.N],
    possession_adjust=PossessionAdjustment.OUT_OF_POSS,
)
INTS_TACKLES = TemplateAttribute(
    "Int+Tackles",
    lambda df: df[fb.INTERCEPTIONS.N] + df[fb.TACKLES.N],
    True,
    columns_used=[fb.INTERCEPTIONS.N, fb.TACKLES.N],
)
KEY_PASSES = TemplateAttribute(
    name="Key Passes",
    calculation=lambda df: df[fb.ASSISTED_SHOTS.N],
    ascending_rank=True,
    columns_used=[fb.ASSISTED_SHOTS.N],
)
NON_PENALTY_GOALS = TemplateAttribute(
    "Non-Penalty Goals",
    lambda df: df[fb.NON_PENALTY_GOALS.N],
    True,
    columns_used=[fb.GOALS.N, fb.PENS_MADE.N],
    sig_figs=2,
)

NPXG = TemplateAttribute("NPxG", lambda df: df[fb.NPXG.N], True, columns_used=[fb.NPXG.N], sig_figs=2)

NPXG_PER_SHOT = TemplateAttribute(
    "NPxG/Shot",
    lambda df: df[fb.NPXG.N] / df[fb.SHOTS_TOTAL.N],
    True,
    columns_used=[fb.NPXG.N, fb.SHOTS_TOTAL.N],
    sig_figs=2,
)

PCT_DRIBBLERS_TACKLED = TemplateAttribute(
    "Pct of Dribblers Tackled",
    lambda df: (100 * df[fb.TACKLES_VS_DRIBBLES_WON.N] / df[fb.TACKLES_VS_DRIBBLES.N]).fillna(0),
    True,
    columns_used=[fb.TACKLES_VS_DRIBBLES_WON.N, fb.TACKLES_VS_DRIBBLES.N],
)
PASSING_PCT = TemplateAttribute(
    name="Passing %",
    calculation=lambda df: 100 * (df[fb.PASSES_COMPLETED.N] / df[fb.PASSES.N]).fillna(0),
    ascending_rank=True,
    columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N],
)
PASSES_PROGRESSIVE = TemplateAttribute(
    "Progressive Passes",
    lambda df: df[fb.PROGRESSIVE_PASSES.N],
    True,
    columns_used=[fb.PROGRESSIVE_PASSES.N],
)
PASSES_PROGRESSIVE_PCT = TemplateAttribute(
    "Progressive Passes %",
    lambda df: 100 * df[fb.PROGRESSIVE_PASSES.N] / df[fb.PASSES.N],
    True,
    columns_used=[fb.PROGRESSIVE_PASSES.N, fb.PASSES.N],
)
SCORING_CONTRIBUTIONS = TemplateAttribute(
    name="Scoring Contributions",
    calculation=lambda df: (df[fb.NON_PENALTY_GOALS.N] + df[fb.ASSISTS.N]),
    ascending_rank=True,
    columns_used=[fb.GOALS.N, fb.ASSISTS.N, fb.PENS_MADE.N],
    sig_figs=2,
)
SHOOTING_PCT = TemplateAttribute(
    "Shooting %",
    lambda df: (100 * df[fb.NON_PENALTY_GOALS.N] / df[fb.SHOTS_TOTAL.N]).fillna(0),
    True,
    columns_used=[fb.GOALS.N, fb.PENS_MADE.N, fb.SHOTS_TOTAL.N],
)
SHOTS = TemplateAttribute("Shots", lambda df: df[fb.SHOTS_TOTAL.N], True, columns_used=[fb.SHOTS_TOTAL.N])

SHOTS_CREATED_OPEN_PLAY = TemplateAttribute(
    "Open Play Shots Created",
    lambda df: df[fb.SCA.N] - df[fb.SCA_PASSES_DEAD.N],
    True,
    columns_used=[fb.SCA.N, fb.SCA_PASSES_DEAD.N],
)

SUCCESSFUL_DRIBBLES = TemplateAttribute(
    "Successful Dribbles",
    lambda df: df[fb.DRIBBLES_COMPLETED.N],
    True,
    columns_used=[fb.DRIBBLES_COMPLETED.N],
)
TACKLES_PADJ = TemplateAttribute(
    "PAdj Tackles",
    lambda df: df[fb.TACKLES.N],
    True,
    columns_used=[fb.TACKLES.N],
    possession_adjust=PossessionAdjustment.OUT_OF_POSS,
)
THROUGHBALLS = TemplateAttribute(
    name="Throughballs",
    calculation=lambda df: df[fb.THROUGH_BALLS.N],
    ascending_rank=True,
    columns_used=[fb.THROUGH_BALLS.N],
    sig_figs=2,
)

TURNOVERS = TemplateAttribute(
    name="Turnovers",
    calculation=lambda df: df[fb.MISCONTROLS.N]
    + df[fb.DISPOSSESSED.N]
    + df[fb.PASSES.N]
    - df[fb.PASSES_COMPLETED.N]
    - (df[fb.CROSSES.N] - df[fb.CROSSES_INTO_PENALTY_AREA.N]),
    ascending_rank=False,
    columns_used=[
        fb.MISCONTROLS.N,
        fb.DISPOSSESSED.N,
        fb.PASSES.N,
        fb.PASSES_COMPLETED.N,
        fb.CROSSES.N,
        fb.CROSSES_INTO_PENALTY_AREA.N,
    ],
)

XA = TemplateAttribute("xA", lambda df: df[fb.XA.N], True, columns_used=[fb.XA.N], sig_figs=2)

PROGRESSIVE_PASSES_RECEIVED = TemplateAttribute(
    "Progressive Passes Received",
    lambda df: df[fb.PROGRESSIVE_PASSES_RECEIVED.N],
    True,
    columns_used=[fb.PROGRESSIVE_PASSES_RECEIVED.N],
)

FOULS_WON = TemplateAttribute(
    "Fouls Won",
    lambda df: df[fb.FOULED.N],
    True,
    columns_used=[fb.FOULED.N],
)

TOUCHES_IN_PEN_AREA = TemplateAttribute(
    "Touches in Penalty Area",
    lambda df: df[fb.TOUCHES_ATT_PEN_AREA.N],
    True,
    columns_used=[fb.TOUCHES_ATT_PEN_AREA.N],
)

HEADED_SHOTS = TemplateAttribute(
    "Headed Shots",
    lambda df: df[fb.HEADED_SHOTS.N],
    True,
    columns_used=[fb.HEADED_SHOTS.N],
)

CARRY_PROGRESSIVE_DISTANCE = TemplateAttribute(
    "Progressive Carry Distance",
    lambda df: df[fb.CARRY_PROGRESSIVE_DISTANCE.N],
    True,
    columns_used=[fb.CARRY_PROGRESSIVE_DISTANCE.N],
)


SELF_CREATED_SHOT_PCT = TemplateAttribute(
    "Self-Created Shot %",
    lambda df: 100 * df[SELF_CREATED_SHOTS.N] / df[fb.SHOTS_TOTAL.N],
    True,
    columns_used=[fb.SHOTS_TOTAL.N],
)


MFTemplate = [
    PASSING_PCT,
    KEY_PASSES,
    THROUGHBALLS,
    SCORING_CONTRIBUTIONS,
    SUCCESSFUL_DRIBBLES,
    CARRY_PROGRESSIVE_DISTANCE,
    TURNOVERS,
    FOULS,
    HEADERS_WON_PCT,
    PCT_DRIBBLERS_TACKLED,
    TACKLES_PADJ,
    INTERCEPTIONS_PADJ,
    PASSES_PROGRESSIVE,
]

CBTemplate = [
    PASSING_PCT,
    PCT_DRIBBLERS_TACKLED,
    TACKLES_PADJ,
    INTERCEPTIONS_PADJ,
    BLOCKS_PADJ,
    CLEARANCES_PADJ,
    FOULS,
    HEADERS_WON,
    HEADERS_WON_PCT,
    PASSES_PROGRESSIVE,
    PASSES_PROGRESSIVE_PCT,
]
FBTemplate = [
    PASSING_PCT,
    TACKLES_PADJ,
    INTERCEPTIONS_PADJ,
    KEY_PASSES,
    CROSSES_COMPLETED,
    CROSSES_PCT,
    SUCCESSFUL_DRIBBLES,
    CARRY_PROGRESSIVE_DISTANCE,
    DISPOSSESSED,
    SCORING_CONTRIBUTIONS,
    PCT_DRIBBLERS_TACKLED,
    FOULS,
]
AttackerTemplate = [
    NON_PENALTY_GOALS,
    SHOTS,
    SHOOTING_PCT,
    PASSING_PCT,
    ASSISTS,
    XA,
    SHOTS_CREATED_OPEN_PLAY,
    INTS_TACKLES,
    TURNOVERS,
    SUCCESSFUL_DRIBBLES,
    SELF_CREATED_SHOT_PCT,
    NPXG,
    NPXG_PER_SHOT,
]

TargetmanTemplate = [
    NON_PENALTY_GOALS,
    NPXG,
    HEADERS_WON,
    HEADERS_WON_PCT,
    HEADED_SHOTS,
    PROGRESSIVE_PASSES_RECEIVED,
    PASSES_PROGRESSIVE,
    PASSING_PCT,
    FOULS_WON,
    TOUCHES_IN_PEN_AREA,
    SHOTS_CREATED_OPEN_PLAY,
    NPXG_PER_SHOT,
]

GoalkeeperTemplate = [
    TemplateAttribute(
        "Goals Conceded",
        lambda df: df[fb.GOALS_AGAINST_GK.N],
        False,
        columns_used=[fb.GOALS_AGAINST_GK.N],
        sig_figs=2,
    ),
    TemplateAttribute(
        "Save %",
        lambda df: 100 * (df[fb.SAVES.N] / df[fb.SHOTS_ON_TARGET_AGAINST.N]).fillna(0),
        True,
        columns_used=[fb.SHOTS_ON_TARGET_AGAINST.N, fb.SAVES.N],
    ),
    TemplateAttribute("Saves", lambda df: df[fb.SAVES.N], True, columns_used=[fb.SAVES.N]),
    TemplateAttribute(
        "PSxG-Goals Conceded",
        lambda df: df[fb.PSXG_GK.N] - df[fb.GOALS_AGAINST_GK.N],
        True,
        columns_used=[fb.PSXG_GK.N, fb.GOALS_AGAINST_GK.N],
        sig_figs=2,
    ),
    TemplateAttribute(
        "Crosses Collected",
        lambda df: df[fb.CROSSES_STOPPED_GK.N],
        True,
        columns_used=[fb.CROSSES_STOPPED_GK.N],
    ),
    TemplateAttribute(
        "Sweeper Actions",
        lambda df: df[fb.DEF_ACTIONS_OUTSIDE_PEN_AREA_GK.N],
        True,
        columns_used=[fb.DEF_ACTIONS_OUTSIDE_PEN_AREA_GK.N],
    ),
    TemplateAttribute(
        "Sweeper Action Distance",
        lambda df: df[fb.AVG_DISTANCE_DEF_ACTIONS_GK.N],
        True,
        columns_used=[fb.AVG_DISTANCE_DEF_ACTIONS_GK.N],
    ),
    TemplateAttribute(
        "Thrown Passes",
        lambda df: df[fb.PASSES_THROWS_GK.N],
        True,
        columns_used=[fb.PASSES_THROWS_GK.N],
    ),
    TemplateAttribute(
        "Long Passes",
        lambda df: df[fb.PASSES_LONG.N],
        True,
        columns_used=[fb.PASSES_LONG.N],
    ),
    TemplateAttribute(
        "Long Pass % Completed",
        lambda df: 100 * (df[fb.PASSES_COMPLETED_LONG.N] / df[fb.PASSES_LONG.N]).fillna(0),
        True,
        columns_used=[fb.PASSES_COMPLETED_LONG.N, fb.PASSES_LONG.N],
    ),
    TemplateAttribute(
        "Short Passes",
        lambda df: df[fb.PASSES_SHORT.N] + df[fb.PASSES_MEDIUM.N],
        True,
        columns_used=[fb.PASSES_SHORT.N, fb.PASSES_MEDIUM.N],
    ),
    TemplateAttribute(
        "Short Pass % Completed",
        lambda df: 100
        * (
            (df[fb.PASSES_COMPLETED_SHORT.N] + df[fb.PASSES_COMPLETED_MEDIUM.N])
            / (df[fb.PASSES_SHORT.N] + df[fb.PASSES_MEDIUM.N])
        ).fillna(0),
        True,
        columns_used=[
            fb.PASSES_COMPLETED_SHORT.N,
            fb.PASSES_COMPLETED_MEDIUM.N,
            fb.PASSES_SHORT.N,
            fb.PASSES_MEDIUM.N,
        ],
    ),
]

TeamTemplate = [
    TemplateAttribute(
        "Open Play NPxG",
        lambda df: df["live_xg_team"] / df["matches"],
        True,
        columns_used=[],
        sig_figs=2,
    ),
    TemplateAttribute(
        "Open Play NPxGA",
        lambda df: df["live_xg_opp"] / df["matches"],
        False,
        columns_used=[],
        sig_figs=2,
    ),
    TemplateAttribute(
        "Set Piece NPxGD",
        lambda df: (df["setpiece_xg_team"] - df["setpiece_xg_opp"]) / df["matches"],
        True,
        columns_used=[],
        sig_figs=2,
    ),
    TemplateAttribute(
        "Big Chance Created",
        lambda df: df["big_chance_team"] / df["matches"],
        True,
        columns_used=[],
    ),
    TemplateAttribute(
        "Shots",
        lambda df: df[fb.SHOTS_TOTAL.N + "_team"] / df["matches"],
        True,
        columns_used=[fb.SHOTS_TOTAL.N],
    ),
    TemplateAttribute(
        "Shots Conceded",
        lambda df: df[fb.SHOTS_TOTAL.N + "_opp"] / df["matches"],
        False,
        columns_used=[fb.SHOTS_TOTAL.N],
    ),
    TemplateAttribute(
        "Cross % Box Entries",
        lambda df: 100
        * df[fb.CROSSES_INTO_PENALTY_AREA.N + "_team"]
        / (
            df[fb.CROSSES_INTO_PENALTY_AREA.N + "_team"]
            + df[fb.PASSES_INTO_PENALTY_AREA.N + "_team"]
            + df[fb.CARRIES_INTO_PENALTY_AREA.N + "_team"]
        ),
        True,
        columns_used=[
            fb.CROSSES_INTO_PENALTY_AREA.N,
            fb.PASSES_INTO_PENALTY_AREA.N,
            fb.CARRIES_INTO_PENALTY_AREA.N,
        ],
    ),
    TemplateAttribute(
        "Long Ball %",
        lambda df: 100 * df[fb.PASSES_LONG.N + "_team"] / df[fb.PASSES.N + "_team"],
        True,
        columns_used=[fb.PASSES_LONG.N, fb.PASSES.N],
    ),
    TemplateAttribute(
        "Possession %",
        lambda df: 100 * df[fb.TOUCHES.N + "_team"] / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"]),
        True,
        columns_used=[fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Final 3rd Tackles",
        lambda df: df[fb.TACKLES_ATT_3RD.N + "_team"]
        / df["matches"]
        / (df[fb.TOUCHES.N + "_opp"] / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"]))
        * 0.5,
        True,
        columns_used=[fb.TACKLES_ATT_3RD.N, fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Fouls Committed",
        lambda df: df[fb.FOULS.N + "_team"]
        / df["matches"]
        / (df[fb.TOUCHES.N + "_opp"] / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"]))
        * 0.5,
        False,
        columns_used=[fb.FOULS.N, fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Dribbles",
        lambda df: df[fb.DRIBBLES.N + "_team"]
        / df["matches"]
        / (df[fb.TOUCHES.N + "_team"] / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"]))
        * 0.5,
        True,
        columns_used=[fb.DRIBBLES.N, fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Offsides",
        lambda df: df[fb.OFFSIDES.N + "_team"]
        / df["matches"]
        / (df[fb.TOUCHES.N + "_team"] / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"]))
        * 0.5,
        False,
        columns_used=[fb.OFFSIDES.N, fb.TOUCHES.N],
    ),
]
CreativityTemplate = [ASSISTS, XA]
