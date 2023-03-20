from typing import Callable
import dataclasses
import enum
import pandas as pd
from footmav import fb

enum.Enum


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


MFTemplate = [
    TemplateAttribute(
        name="Passing %",
        calculation=lambda df: 100* (df[fb.PASSES_COMPLETED.N] / df[fb.PASSES.N]).fillna(0),
        ascending_rank=True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N],
        
    ),
    TemplateAttribute(
        name="Key Passes",
        calculation=lambda df: df[fb.ASSISTED_SHOTS.N],
        ascending_rank=True,
        columns_used=[fb.ASSISTED_SHOTS.N],
    ),
    TemplateAttribute(
        name="Throughballs",
        calculation=lambda df: df[fb.THROUGH_BALLS.N],
        ascending_rank=True,
        columns_used=[fb.THROUGH_BALLS.N],
    ),
    TemplateAttribute(
        name="Scoring Contributions",
        calculation=lambda df: (df[fb.NON_PENALTY_GOALS.N] + df[fb.ASSISTS.N]),
        ascending_rank=True,
        columns_used=[fb.GOALS.N, fb.ASSISTS.N, fb.PENS_MADE.N],
    ),
    TemplateAttribute(
        "Successful Dribbles",
        lambda df: df[fb.DRIBBLES_COMPLETED.N],
        True,
        columns_used=[fb.DRIBBLES_COMPLETED.N],
    ),
    TemplateAttribute(
        "Dispossessed",
        lambda df: df[fb.DISPOSSESSED.N],
        False,
        columns_used=[fb.DISPOSSESSED.N],
    ),
    TemplateAttribute(
        "Fouls", lambda df: df[fb.FOULS.N], False, columns_used=[fb.FOULS.N]
    ),
    TemplateAttribute(
        "Pct of Dribblers Tackled",
        lambda df: (
            100* df[fb.TACKLES_VS_DRIBBLES_WON.N] / df[fb.TACKLES_VS_DRIBBLES.N]
        ).fillna(0),
        True,
        columns_used=[fb.TACKLES_VS_DRIBBLES_WON.N, fb.TACKLES_VS_DRIBBLES.N],
        
    ),
    TemplateAttribute(
        "PAdj Tackles",
        lambda df: df[fb.TACKLES.N],
        True,
        columns_used=[fb.TACKLES.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "PAdj Interceptions",
        lambda df: df[fb.INTERCEPTIONS.N],
        True,
        columns_used=[fb.INTERCEPTIONS.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "Progressive Passes",
        lambda df: df[fb.PROGRESSIVE_PASSES.N],
        True,
        columns_used=[fb.PROGRESSIVE_PASSES.N],
    ),
]

CBTemplate = [
    TemplateAttribute(
        name="Passing %",
        calculation=lambda df:100* (df[fb.PASSES_COMPLETED.N] / df[fb.PASSES.N]).fillna(0),
        ascending_rank=True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N],
        
    ),
    TemplateAttribute(
        "Pct of Dribblers Tackled",
        lambda df: 100*(
            df[fb.TACKLES_VS_DRIBBLES_WON.N] / df[fb.TACKLES_VS_DRIBBLES.N]
        ).fillna(0),
        True,
        columns_used=[fb.TACKLES_VS_DRIBBLES_WON.N, fb.TACKLES_VS_DRIBBLES.N],
        
    ),
    TemplateAttribute(
        "PAdj Tackles",
        lambda df: df[fb.TACKLES.N],
        True,
        columns_used=[fb.TACKLES.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "PAdj Interceptions",
        lambda df: df[fb.INTERCEPTIONS.N],
        True,
        columns_used=[fb.INTERCEPTIONS.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "PAdj Blocks",
        lambda df: df[fb.BLOCKS.N],
        True,
        columns_used=[fb.BLOCKS.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "PAdj Clearances",
        lambda df: df[fb.CLEARANCES.N],
        True,
        columns_used=[fb.CLEARANCES.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "Fouls", lambda df: df[fb.FOULS.N], False, columns_used=[fb.FOULS.N]
    ),
    TemplateAttribute(
        "Headers Won",
        lambda df: df[fb.AERIALS_WON.N],
        True,
        columns_used=[fb.AERIALS_WON.N],
    ),
    TemplateAttribute(
        "Headers Won %",
        lambda df: (
            100 * df[fb.AERIALS_WON.N] / (df[fb.AERIALS_WON.N] + df[fb.AERIALS_LOST.N])
        ).fillna(0),
        True,
        columns_used=[fb.AERIALS_WON.N, fb.AERIALS_LOST.N],
        
    ),
    TemplateAttribute(
        "Progressive Passes",
        lambda df: df[fb.PROGRESSIVE_PASSES.N],
        True,
        columns_used=[fb.PROGRESSIVE_PASSES.N],
    ),
    TemplateAttribute(
        "Progressive Passes %",
        lambda df: 100 * df[fb.PROGRESSIVE_PASSES.N] / df[fb.PASSES.N],
        True,
        columns_used=[fb.PROGRESSIVE_PASSES.N, fb.PASSES.N],
        
    ),
]
FBTemplate = [
    TemplateAttribute(
        name="Passing %",
        calculation=lambda df: 100* (df[fb.PASSES_COMPLETED.N] / df[fb.PASSES.N]).fillna(0),
        ascending_rank=True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N],
        
    ),
    TemplateAttribute(
        "PAdj Tackles",
        lambda df: df[fb.TACKLES.N],
        True,
        columns_used=[fb.TACKLES.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        "PAdj Interceptions",
        lambda df: df[fb.INTERCEPTIONS.N],
        True,
        columns_used=[fb.INTERCEPTIONS.N],
        possession_adjust=PossessionAdjustment.OUT_OF_POSS,
    ),
    TemplateAttribute(
        name="Key Passes",
        calculation=lambda df: df[fb.ASSISTED_SHOTS.N],
        ascending_rank=True,
        columns_used=[fb.ASSISTED_SHOTS.N],
    ),
    TemplateAttribute(
        name="Completed Crosses",
        calculation=lambda df: df[fb.CROSSES_INTO_PENALTY_AREA.N],
        ascending_rank=True,
        columns_used=[fb.CROSSES_INTO_PENALTY_AREA.N],
    ),
    TemplateAttribute(
        name="Crosses %",
        calculation=lambda df: (
            100 * df[fb.CROSSES_INTO_PENALTY_AREA.N] / df[fb.CROSSES.N]
        ).fillna(0),
        ascending_rank=True,
        columns_used=[fb.CROSSES_INTO_PENALTY_AREA.N, fb.CROSSES.N],
        
    ),
    TemplateAttribute(
        "Successful Dribbles",
        lambda df: df[fb.DRIBBLES_COMPLETED.N],
        True,
        columns_used=[fb.DRIBBLES_COMPLETED.N],
    ),
    TemplateAttribute(
        "Dispossessed",
        lambda df: df[fb.DISPOSSESSED.N],
        False,
        columns_used=[fb.DISPOSSESSED.N],
    ),
    TemplateAttribute(
        name="Scoring Contributions",
        calculation=lambda df: (df[fb.NON_PENALTY_GOALS.N] + df[fb.ASSISTS.N]),
        ascending_rank=True,
        columns_used=[fb.GOALS.N, fb.ASSISTS.N, fb.PENS_MADE.N],
    ),
    TemplateAttribute(
        "Pct of Dribblers Tackled",
        lambda df: 100* (
            df[fb.TACKLES_VS_DRIBBLES_WON.N] / df[fb.TACKLES_VS_DRIBBLES.N]
        ).fillna(0),
        True,
        columns_used=[fb.TACKLES_VS_DRIBBLES_WON.N, fb.TACKLES_VS_DRIBBLES.N],
        
    ),
    TemplateAttribute(
        "Fouls", lambda df: df[fb.FOULS.N], False, columns_used=[fb.FOULS.N]
    ),
]
AttackerTemplate = [
    TemplateAttribute(
        "Non-Penalty Goals",
        lambda df: df[fb.NON_PENALTY_GOALS.N],
        True,
        columns_used=[fb.GOALS.N, fb.PENS_MADE.N],
    ),
    TemplateAttribute(
        "Shots", lambda df: df[fb.SHOTS_TOTAL.N], True, columns_used=[fb.SHOTS_TOTAL.N]
    ),
    TemplateAttribute(
        "Shooting %",
        lambda df: (100 * df[fb.NON_PENALTY_GOALS.N] / df[fb.SHOTS_TOTAL.N]).fillna(0),
        True,
        columns_used=[fb.GOALS.N, fb.PENS_MADE.N, fb.SHOTS_TOTAL.N],
        
    ),
    TemplateAttribute(
        "Passing %",
        lambda df: (100 * df[fb.PASSES_COMPLETED.N] / df[fb.PASSES.N]).fillna(0),
        True,
        columns_used=[fb.PASSES_COMPLETED.N, fb.PASSES.N],
        
    ),
    TemplateAttribute(
        "Assists", lambda df: df[fb.ASSISTS.N], True, columns_used=[fb.ASSISTS.N]
    ),
    TemplateAttribute("xA", lambda df: df[fb.XA.N], True, columns_used=[fb.XA.N]),
    TemplateAttribute(
        "Open Play Shots Created",
        lambda df: df[fb.SCA.N] - df[fb.SCA_PASSES_DEAD.N],
        True,
        columns_used=[fb.SCA.N, fb.SCA_PASSES_DEAD.N],
    ),
    TemplateAttribute(
        "Int+Tackles",
        lambda df: df[fb.INTERCEPTIONS.N] + df[fb.TACKLES.N],
        True,
        columns_used=[fb.INTERCEPTIONS.N, fb.TACKLES.N],
    ),
    TemplateAttribute(
        "Dispossessed",
        lambda df: df[fb.DISPOSSESSED.N],
        False,
        columns_used=[fb.DISPOSSESSED.N],
    ),
    TemplateAttribute(
        "Successful Dribbles",
        lambda df: df[fb.DRIBBLES_COMPLETED.N],
        True,
        columns_used=[fb.DRIBBLES_COMPLETED.N],
    ),
    TemplateAttribute("NPxG", lambda df: df[fb.NPXG.N], True, columns_used=[fb.NPXG.N]),
    TemplateAttribute(
        "NPxG/Shot",
        lambda df: df[fb.NPXG.N]/df[fb.SHOTS_TOTAL.N],
        True,
        columns_used=[fb.NPXG.N,fb.SHOTS_TOTAL.N]
    ),
]

GoalkeeperTemplate = [
    TemplateAttribute(
        "Goals Conceded",
        lambda df: df[fb.GOALS_AGAINST_GK.N],
        False,
        columns_used=[fb.GOALS_AGAINST_GK.N],
    ),
    TemplateAttribute(
        "Save %",
        lambda df: 100 * (df[fb.SAVES.N] / df[fb.SHOTS_ON_TARGET_AGAINST.N]).fillna(0),
        True,
        columns_used=[fb.SHOTS_ON_TARGET_AGAINST.N, fb.SAVES.N],
        
    ),
    TemplateAttribute(
        "Saves", lambda df: df[fb.SAVES.N], True, columns_used=[fb.SAVES.N]
    ),
    TemplateAttribute(
        "PSxG-Goals Conceded",
        lambda df: df[fb.PSXG_GK.N] - df[fb.GOALS_AGAINST_GK.N],
        True,
        columns_used=[fb.PSXG_GK.N, fb.GOALS_AGAINST_GK.N],
    ),
    TemplateAttribute(
        "Crosses Collected",
        lambda df: df[fb.CROSSES_STOPPED_GK.N] ,
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
        lambda df: 100*(df[fb.PASSES_COMPLETED_LONG.N] / df[fb.PASSES_LONG.N]).fillna(0),
        False,
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
        lambda df: 100*(
            (df[fb.PASSES_COMPLETED_SHORT.N] + df[fb.PASSES_COMPLETED_MEDIUM.N])
            / (df[fb.PASSES_SHORT.N] + df[fb.PASSES_MEDIUM.N])
        ).fillna(0),
        False,
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
    ),
    TemplateAttribute(
        "Open Play NPxGA",
        lambda df: df["live_xg_opp"] / df["matches"],
        False,
        columns_used=[],
    ),
    TemplateAttribute(
        "Set Piece NPxGD",
        lambda df: (df["setpiece_xg_team"] - df["setpiece_xg_opp"]) / df["matches"],
        True,
        columns_used=[],
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
        lambda df: 100* df[fb.CROSSES_INTO_PENALTY_AREA.N + "_team"]
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
        lambda df: 100*df[fb.PASSES_LONG.N + "_team"] / df[fb.PASSES.N + "_team"],
        True,
        columns_used=[fb.PASSES_LONG.N, fb.PASSES.N],
        
    ),
    TemplateAttribute(
        "Possession %",
        lambda df:100* df[fb.TOUCHES.N + "_team"]
        / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"]),
        True,
        columns_used=[fb.TOUCHES.N],
        
    ),
    TemplateAttribute(
        "PAdj Final 3rd Tackles",
        lambda df: df[fb.TACKLES_ATT_3RD.N + "_team"]
        / df["matches"]
        / (
            df[fb.TOUCHES.N + "_opp"]
            / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"])
        )
        * 0.5,
        True,
        columns_used=[fb.TACKLES_ATT_3RD.N, fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Fouls Committed",
        lambda df: df[fb.FOULS.N + "_team"]
        / df["matches"]
        / (
            df[fb.TOUCHES.N + "_opp"]
            / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"])
        )
        * 0.5,
        False,
        columns_used=[fb.FOULS.N, fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Dribbles",
        lambda df: df[fb.DRIBBLES.N + "_team"]
        / df["matches"]
        / (
            df[fb.TOUCHES.N + "_team"]
            / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"])
        )
        * 0.5,
        True,
        columns_used=[fb.DRIBBLES.N, fb.TOUCHES.N],
    ),
    TemplateAttribute(
        "PAdj Offsides",
        lambda df: df[fb.OFFSIDES.N + "_team"]
        / df["matches"]
        / (
            df[fb.TOUCHES.N + "_team"]
            / (df[fb.TOUCHES.N + "_opp"] + df[fb.TOUCHES.N + "_team"])
        )
        * 0.5,
        False,
        columns_used=[fb.OFFSIDES.N, fb.TOUCHES.N],
    ),
]
