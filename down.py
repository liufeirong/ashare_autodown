import tushare as ts
import pandas as pd
from sqlalchemy.types import CHAR

from utils.db_util import engine, execute
from utils.time_util import get_last_date
from utils.market_util import get_hs300_last_date_online,get_hs300_last_date_offline 
from data.market import get_live_data_day

sql_start_date = 'SELECT code, max(datetime) as start_date FROM stock.t_{} GROUP BY code'
sql_validate = "SELECT DISTINCT code FROM t_{} WHERE datetime='{}'"
sql_delete = "DELETE FROM t_{} WHERE datetime>='{}';"

def get_valid_tickers(last_date, freq='d'):
    df_validate = pd.read_sql(sql_validate.format(freq, last_date), engine)
    return df_validate['code'].values.tolist()

def call_back(row, conn, asset, end_date, freq):
    ticker, start_date = row[0], row[1]

    if start_date is not None and end_date is not None and start_date >= pd.to_datetime(end_date):
#         print("{}'s data is the newest".format(ticker))
        return
    
    data = ts.bar(ticker, conn=conn, asset=asset, start_date=start_date, end_date=end_date,  freq=freq)
    if data is None or data.empty:
        print("down {} is None or empty".format(ticker))
        return
#     print("down {} start from {}".format(ticker, start_date))
    
    data = data.iloc[::-1].iloc[1:].reset_index()
    data = data.set_index(['datetime', 'code'])
    data.to_sql("t_{}".format(freq), engine, if_exists='append', index=True,  dtype = {'code':CHAR(6)})
    
def down(tickers=None, asset='E', freq='d', start_date='2015-01-01', end_date=None):
    if tickers is None:
        df_basics = ts.get_stock_basics()
        tickers = df_basics.index.sort_values()

    df_sql = pd.read_sql(sql_start_date.format(freq), engine, index_col=['code'], parse_dates=['start_date'])
    df_start_date = pd.DataFrame(index=tickers).join(df_sql)
    df_start_date = df_start_date.fillna(pd.to_datetime(start_date, format='%Y-%m-%d')).reset_index()

    conn = ts.get_apis()
    df_start_date.apply(lambda row:call_back(row, conn, asset, end_date, freq), axis=1)
    ts.close_apis(conn)
    
def auto_down(start_date='1990-01-01', freq='d', re_down=False):
    if re_down:
        execute(sql_delete.format(freq, start_date))
        
    last_date0, last_date1 = get_hs300_last_date_online()
    last_date_offline = get_hs300_last_date_offline()
    last_date = get_last_date()
    if last_date0 is None:
        print("down benchmark failure!")
        return
    if last_date_offline == min(last_date0, last_date):
        print('market data is the newest!')
        return

    if last_date0 == last_date:
        down(freq=freq, start_date=start_date, end_date=last_date1)
        down(['000300'], 'INDEX', freq=freq, start_date=start_date, end_date=last_date0)
        tickers = get_valid_tickers(last_date1, freq=freq)
        data = get_live_data_day(tickers).set_index(['datetime','code'])
        data.to_sql("t_{}".format(freq), engine, if_exists='append', index=True)
    else:
        last_date = min(last_date0, last_date)
        down(freq=freq, start_date=start_date, end_date=last_date)
        down(['000300'], 'INDEX', freq=freq, start_date=start_date,end_date=last_date)
        
auto_down(start_date='1990-01-01', freq='d', re_down=False)