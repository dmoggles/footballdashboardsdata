from footballdashboardsdata.datasource import DataSource


data = DataSource.get_data(
    "AMPizza", player_name="joao felix", team="chelsea", leagues=["Premier League"], season=2022, use_all_minutes=True
)
print(data)
