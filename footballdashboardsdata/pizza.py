from dbconnect.connector import Connection
import pandas as pd
from typing import List
from footmav import FbRefData, fb, aggregate_by, filter, filters, Filter, per_90
from footmav.operations.possession_adjust import possession_adjust
from footballdashboardsdata.datasource import DataSource
from footballdashboardsdata.utils import possession_adjust
from footballdashboardsdata.templates import MFTemplate, CBTemplate, FBTemplate, AttackerTemplate, TemplateAttribute, PossessionAdjustment
from abc import abstractmethod



class PizzaDataSource(DataSource):

    @abstractmethod
    def get_template(self)->List[TemplateAttribute]:
        pass
    @abstractmethod
    def get_comparison_positions(self)->List[str]:
        pass

    def _specific_position_impl(self, data: pd.DataFrame) -> dict:
        data = {
            attrib.name:attrib.calculation(data).rank(pct=True, method='min', ascending=attrib.ascending_rank)
            for attrib in self.get_template()
        }
        return data

    def get_data_dict(self, data):
        specific_data = self._specific_position_impl(data)
        data_dict= {
            'Player': data[fb.PLAYER.N].tolist(),
            'Team': data[fb.TEAM.N].tolist(),
            'Minutes':data[fb.MINUTES.N].tolist(),
            'Competition':data[fb.COMPETITION.N].tolist(),
            'Season':data[fb.YEAR.N].tolist(),
        }
        data_dict.update(specific_data)
        return data_dict


    def _get_decorated_team_name(self, team_name:str, gender:str)->str:
        query = f"""
        SELECT decorated_name FROM mclachbot_teams WHERE team_name = '{team_name}'
        AND gender='{gender}'
        """
        result =  Connection('M0neyMa$e').query(query)
        if len(result) == 0:
            return team_name
        return result['decorated_name'].values[0]
    def _get_decorated_league_name(self, league_name:str)->str:
        query = f"""
        SELECT decorated_name FROM mclachbot_leagues WHERE league_name = '{league_name}'
        """
        result = Connection('M0neyMa$e').query(query)
        if len(result) == 0:
            return league_name
        return result['decorated_name'].values[0]


    def impl_get_data(self, player_name:str, leagues:List[str], team:str, season:int)->pd.DataFrame:
        template = self.get_template()
        all_template_columns = [attr.columns_used for attr in template]
        all_template_columns = [item for sublist in all_template_columns for item in sublist]
        all_template_columns = list(set(all_template_columns))
        all_columns = [
            fb.PLAYER_ID.N,
            fb.PLAYER.N,
            fb.DATE.N,
            fb.TEAM.N,
            fb.OPPONENT.N,
            fb.MINUTES.N,
            fb.COMPETITION.N,
            fb.YEAR.N,
            fb.ENRICHED_POSITION.N,
            fb.TOUCHES.N,
            'gender',
            'match_id',
            'dob',
        ]+all_template_columns
        league_str = ','.join([f"'{league}'" for league in leagues])
        query = f"""
        SELECT `{'`,`'.join(all_columns)}` 
        FROM fbref WHERE comp in ({league_str}) AND season = {season}
        """
        orig_df = Connection('M0neyMa$e').query(query)
        gender = orig_df['gender'].iloc[0]

        adjust_factors = possession_adjust.adj_possession_factors(orig_df)
        orig_df = orig_df.merge(adjust_factors, on=[fb.COMPETITION.N, fb.TEAM.N, fb.YEAR.N], how='left')
        adjusted_columns = []
        for attribute in template:
            if attribute.possession_adjust==PossessionAdjustment.IN_POSS:
                for col in attribute.columns_used:
                    if col not in adjusted_columns:
                        
                        orig_df[col] = orig_df[col] * orig_df['in_possession_factor']
                        adjusted_columns.append(col)
            elif attribute.possession_adjust==PossessionAdjustment.OUT_OF_POSS:
                for col in attribute.columns_used:
                    if col not in adjusted_columns:
                        orig_df[col] = orig_df[col] * orig_df['out_of_possession_factor']
                        adjusted_columns.append(col)

        fbref_data = FbRefData(orig_df)

        transformed_data = fbref_data.pipe(
            filter, [Filter(fb.ENRICHED_POSITION,self.get_comparison_positions(),  filters.IsIn)]
        ).pipe(
            aggregate_by, [fb.PLAYER_ID,fb.TEAM]
        ).pipe(
            per_90
        )



        df = transformed_data.pipe(
            filter, [Filter(fb.MINUTES, transformed_data.df[fb.MINUTES.N].max()/3., filters.GTE)]
        ).df
        if player_name not in df[fb.PLAYER.N].unique():
            df_player = transformed_data.pipe(
                filter, [Filter(fb.PLAYER, player_name, filters.EQ)]
            ).df
            df=pd.concat([df, df_player])

        

        data_dict = self.get_data_dict(df)
        output = pd.DataFrame(
            data_dict
        )
        
        output_row =  output.loc[(output['Player']==player_name)&(output['Team']==team)].copy()
        player_dob = orig_df.loc[(orig_df[fb.PLAYER.N]==player_name)&(orig_df[fb.TEAM.N]==team), 'dob'].iloc[0]
        if player_dob != pd.Timestamp(1900,1,1):
            output_row['Age']=int((max(orig_df[fb.DATE.N])-player_dob).days/365)
        else:
            output_row['Age']=None
        output_row['image_team']=output_row['Team']
        output_row['image_league']=output_row['Competition']
        output_row['Team'] = self._get_decorated_team_name(output_row['Team'].iloc[0], gender)
        output_row['Competition'] = self._get_decorated_league_name(output_row['Competition'].iloc[0])
        return output_row
        

class MidfieldPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return MFTemplate
    @classmethod
    def get_name(cls) -> str:
        return 'CMPizza'

    def get_comparison_positions(self) -> List[str]:
        return ['CM','DM']

    
        
class CBPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return CBTemplate
    @classmethod
    def get_name(cls) -> str:
        return 'CBPizza'

    def get_comparison_positions(self) -> List[str]:
        return ['CB']

    
        
class FBPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return FBTemplate

    @classmethod
    def get_name(cls) -> str:
        return 'FBPizza'

    def get_comparison_positions(self) -> List[str]:
        return ['LWB','RWB','WB','RB','LB']

    

class FWPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return AttackerTemplate

    @classmethod
    def get_name(cls) -> str:
        return 'FWPizza'

    def get_comparison_positions(self) -> List[str]:
        return ['FW']

    
class AMPizzaDataSource(PizzaDataSource):
    def get_template(self) -> List[TemplateAttribute]:
        return AttackerTemplate

    @classmethod
    def get_name(cls) -> str:
        return 'AMPizza'

    def get_comparison_positions(self) -> List[str]:
        return ['AM','LW','RW','RM','LM']

    