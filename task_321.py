import pandas as pd

file_name = "vacancies.csv"
data_years = pd.read_csv(file_name)
data_years["years"] = data_years["published_at"].apply(lambda date: int(date[:4]))
all_years = list(data_years["years"].unique())
for one_year in all_years:
    data = data_years[data_years["years"] == one_year]
    data.iloc[:, :6].to_csv(f"created_csv2\\file_{one_year}.csv", index = False)
