from footballdashboardsdata.datasource import DataSource
from dbconnect.connector import Connection


class ShotDataSource(DataSource):
    @classmethod
    def get_name(cls) -> str:
        return "ShotData"

    def impl_get_data(self, match_id: str):
        conn = Connection("M0neyMa$e")
        query = f"""
            SELECT * FROM fbref_shots WHERE match_id = '{match_id}'
        """
        return conn.query(query)
