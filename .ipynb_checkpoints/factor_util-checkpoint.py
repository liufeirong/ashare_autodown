import numpy as np
import pandas as pd
import datetime
import talib
import tushare as ts
from data.market import market_data


def get_features(tickers, start_date, end_date, factor_list, seq_len, predict_len, expected_rate=5, freq='d', offset=0,training=False):
    ticker_list = []
    date_list = []
    series_list = []
    label_list = []
    raw_data = raw_data_dict[freq]
    for factor in factor_list:
        series_list.append([])
    trade_dates = raw_data_dict['d'].loc['000001'].loc[start_date:end_date].index.strftime('%Y-%m-%d').unique()
    for ticker in tickers:
        if ticker not in raw_data.index.levels[0]:
            continue
        data = raw_data.loc[ticker].copy()
        data['ma5'] = talib.MA(data['close'], 5)
        data = data.dropna()
        for date in trade_dates:
            idx_date = len(data.loc[:date]) - offset
            if (predict_len > 0 and idx_date+predict_len > len(data)) or (idx_date < seq_len):
                continue
            series = data.iloc[idx_date - seq_len:idx_date]
            for i, factor in enumerate(factor_list):
                if factor in ['close', 'open', 'high', 'low', 'ma5']:
                    values = series[factor]/series['close'].shift(1)
                    values[0] = 1
                elif factor in ['iopen', 'ilow', 'ihigh', 'ima5']:
                    values = series[factor[1:]]/series['close']
                elif factor == 'vol':
                    values = np.log(series[factor]/series[factor].iloc[0])
                else:
                    values = series[factor]
                if training:
                    values = values + np.random.normal(0, 0.1*values.std(), values.shape)
                series_list[i].append(values.values)
            ticker_list.append(ticker)
            date_list.append(date)
            if predict_len > 0:
                label_series = data.iloc[idx_date:idx_date+predict_len]
                label = label_series['high'].iloc[offset:predict_len].max()/series['close'].iloc[-1]*100-100
#                 label = label_series['close'].iloc[predict_len-1]/series['close'].iloc[-1]*100 - 100
                label = 1 if label > expected_rate else 0
                label_list.append(label)
    df_features = pd.DataFrame(index=[date_list, ticker_list])
    for i, factor in enumerate(factor_list):
        df_features[factor + '_' + freq] = series_list[i]
    if predict_len > 0:
        return df_features, pd.Series(label_list, index=[date_list, ticker_list])
    return df_features