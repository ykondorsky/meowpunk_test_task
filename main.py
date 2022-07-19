import sqlite3
import pandas as pd
from datetime import datetime, timedelta
from memory_profiler import profile

REQUESTED_DATE = "06.01.2021"
CLIENT_CSV = "./task/client.csv"
SERVER_CSV = "./task/server.csv"
CHEATERS_DB = "./task/cheaters.db"
OUTPUT_DB = "output.db"

class Reader:
    def __init__(self, date=datetime.now()):
        self._current_day_ts = int(round(date.timestamp()))
        next_day_date = date + timedelta(days=1)
        self._next_day_ts = int(round(next_day_date.timestamp()))

    def read_csv_to_df(self, path: str) -> pd.DataFrame:
        df = pd.read_csv(path)
        # выбрать записи, соответствующие заданной дате
        df = df.loc[(df['timestamp'] >= self._current_day_ts) & (df['timestamp'] < self._next_day_ts)]
        return df

    def read_csv_data(self):
        self._client = self.read_csv_to_df(CLIENT_CSV)
        self._server = self.read_csv_to_df(SERVER_CSV)

        # получить из server_timestamp datetime начала дня (00:00), 
        # чтобы потом сравнить с ban_time из таблицы cheaters
        self._server['server_date'] = self._server['timestamp'] \
            .apply(lambda x: datetime.fromtimestamp(x) \
                .replace(hour=0, minute=0, second=0, microsecond=0))
        # чтобы отличать столбцы в табличке client и server
        self._server = self._server.rename(columns={
            'timestamp' : 'server_ts',
            'description' : 'server_description'
        })

    def merge_dfs(self):
        self._merged = self._client.merge(self._server, on='error_id')

    def read_cheaters_db(self):
        cheaters = sqlite3.connect(CHEATERS_DB)
        cursor = cheaters.cursor()
        cursor.execute("SELECT * FROM cheaters")
        cheaters_data = cursor.fetchall()
        cheaters.close()

        self._cheaters = pd.DataFrame(cheaters_data, columns=['player_id', 'ban_time'])
        # преобразовать object в datetime
        self._cheaters['ban_time'] = self._cheaters['ban_time'] \
            .apply(lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"))

    def filter_cheaters(self):
        # выбрать записи, соответствующие условию для исключения
        to_exclude = self._merged \
            .merge(self._cheaters, on='player_id') \
            .query('ban_time < server_date') \

        # получить новый отфильтрованный датафрейм
        # сразу переименовать колонки, чтобы потом оложить в db-файл
        self._filtered = self._merged[~self._merged.index.isin(to_exclude.index)] \
            .drop(columns=['timestamp', 'server_date']) \
            .rename(columns={
                'server_ts' : 'timestamp',
                'server_description' : 'json_server',
                'description' : 'json_client'
            })

    def output_results(self):
        output_db = sqlite3.connect(OUTPUT_DB)
        cursor = output_db.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS report (
            timestamp integer,
            player_id integer,
            event_id integer,
            error_id text,
            json_server text,
            json_client text
        )""")

        self._filtered.to_sql(name='report', con=output_db, if_exists='append', index=False)
        output_db.close()

    @profile
    def process(self):
        self.read_csv_data()        # считать данные из csv-файлов
        self.merge_dfs()            # объединить датафреймы
        self.read_cheaters_db()     # считать БД cheaters
        self.filter_cheaters()      # отфильтровать согласно условию
        self.output_results()       # выгрузить в выходную БД


if __name__ == '__main__':
    datetime_obj = datetime.strptime(REQUESTED_DATE, "%d.%m.%Y")
    reader = Reader(datetime_obj)
    reader.process()
