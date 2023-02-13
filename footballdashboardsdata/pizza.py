from dbconnect.connector import Connection
import pandas as pd
from typing import List
from footmav import FbRefData, fb, aggregate_by, filter, filters, Filter, per_90
from footmav.operations.possession_adjust import possession_adjust
from footballdashboardsdata.datasource import DataSource
from footballdashboardsdata.utils import possession_adjust
from footballdashboardsdata.templates import MFTemplate, CBTemplate, FBTemplate, AttackerTemplate, TemplateAttribute
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
            'match_id',
        ]+all_template_columns
        league_str = ','.join([f"'{league}'" for league in leagues])
        query = f"""
        SELECT `{'`,`'.join(all_columns)}` FROM fbref WHERE comp in ({league_str}) AND season = {season}
        """
        df = Connection('M0neyMa$e').query(query)

        adjust_factors = possession_adjust.adj_possession_factors(df)

        fbref_data = FbRefData(df)
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

        df = df.merge(adjust_factors, on=[fb.COMPETITION.N, fb.TEAM.N, fb.YEAR.N], how='left')

        data_dict = self.get_data_dict(df)
        output = pd.DataFrame(
            data_dict
        )

        return output.loc[(output['Player']==player_name)&(output['Team']==team)]
        

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

    