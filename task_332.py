import pandas as pd
import requests
import xml.etree.ElementTree as ET

class ProcessCurrencies:
    def __init__(self, file_name: str) -> None:
        self.df = pd.read_csv(file_name)
        self.min_date = self.df["published_at"].min()
        self.max_date = self.df["published_at"].max()
        self.currencies_to_convert = None
        self.currencies_data = None

    def create_row(self, month: str, year: str) -> list or None:
        try:
            format_month = ('0' + str(month))[-2:]
            url = f'https://www.cbr.ru/scripts/XML_daily.asp?date_req=02/{format_month}/{year}'
            resque = requests.get(url)
            tree = ET.fromstring(resque.content)
            row = [f'{year}-{format_month}']
            for value in self.currencies_to_convert:
                if value == 'RUR':
                    row.append(1)
                    continue
                found = False
                for valute in tree:
                    if valute[1].text == value:
                        row.append(round(float(valute[4].text.replace(',', '.')) / float(valute[2].text.replace(',', '.')), 6))
                        found = True
                        break
                if not found:
                    row.append(None)
            return row
        except Exception:
            return None

    def create_currencies_convert(self, n=5000) -> list:
        result = []
        currency_counts = self.df["salary_currency"].value_counts()
        for currency, count in currency_counts.items():
            print(currency, count)
            if count > n:
                result.append(currency)
        self.currencies_to_convert = result
        return result

    def generate_currency(self, begin_date: str, end_date: str) -> None:
        first_year = int(begin_date[:4])
        last_year = int(end_date[:4])
        first_month = int(begin_date[5:7])
        last_month = int(end_date[5:7])
        dataframe = pd.DataFrame(columns = ["date"] + self.currencies_to_convert)
        for year in range(first_year, last_year + 1):
            for month in range(1, 13):
                if (year == first_year and month < first_month) or (year == last_year and month > last_month):
                    continue
                row = self.create_row(month, year)
                if row is None:
                    continue
                dataframe.loc[len(dataframe.index)] = row
        self.currencies_data = dataframe
        dataframe.to_csv('dataframe.csv')

class ProcessSalaries:
    def __init__(self, file_name: str) -> None:
        self.file_name = file_name
        self.currencies = pd.read_csv('dataframe.csv')
        self.available_currencies = list(self.currencies.keys()[2:])

    def process_salaries(self) -> None:
        salaries = []
        to_delete = []
        df = pd.read_csv(self.file_name)
        for row in df.itertuples():
            salary_from = str(row[2])
            salary_to = str(row[3])
            if salary_from != 'nan' and salary_to != 'nan':
                salary = float(salary_from) + float(salary_to)
            elif salary_from != 'nan' and salary_to == 'nan':
                salary = float(salary_from)
            elif salary_from == 'nan' and salary_to != 'nan':
                salary = float(salary_to)
            else:
                to_delete.append(int(row[0]))
                continue
            if row[4] != 'RUR':
                date = row[6][:7]
                multiplier = self.currencies[self.currencies['date'] == date][row[4]].iat[0]
                salary *= multiplier
            if row[4] == 'nan' or row[4] not in self.available_currencies:
                to_delete.append(int(row[0]))
                continue
            salaries.append(salary)
        df.drop(labels=to_delete, axis=0, inplace=True)
        df.drop(labels=['salary_to', 'salary_from', 'salary_currency'], axis = 1, inplace = True)
        df['salary'] = salaries
        df.head(100).to_csv('currency_conversion.csv')

file_name = "vacancies_dif_currencies.csv"
process_cur = ProcessCurrencies(file_name)
process_cur.create_currencies_convert()
process_cur.generate_currency(process_cur.min_date, process_cur.max_date)
ProcessSalaries(file_name).process_salaries()