from zipfile import ZipFile
from urllib.request import urlopen
from functools import reduce
from glob import glob
from pprint import pprint
from bs4 import BeautifulSoup

import pandas as pd
import numpy as np
import datetime
from pandas.core.tools.datetimes import to_datetime
import requests
import io
import os
import csv
import pytz
import sqlalchemy
from sqlalchemy.sql import text
import psycopg2
from OpenSSL import crypto

ISOs = ['ERCOT', 'MISO', 'NYISO', 'ISONE', 'CAISO']
ALLOWED_RT_LAST_DATE_DELTA = {
    'ERCOT': datetime.timedelta(days=0),
    'MISO': datetime.timedelta(days=3),
    'NYISO': datetime.timedelta(days=0),
    'ISONE': datetime.timedelta(days=3),
    'CAISO': datetime.timedelta(days=0)
}
TIMEZONES = {
    'ERCOT': 'US/Central',
    'MISO': 'US/Central',
    'NYISO': 'US/Eastern',
    'ISONE': 'US/Eastern',
    'CAISO': 'US/Pacific'
}
EXCEPTION_DATES = {
    'ERCOT': [],
    'MISO': [],
    'NYISO': [],
    'ISONE': [
        '20170722', '20170724', '20170725', '20170728', '20170730', '20170731', '20170801',
        '20170807', '20171105', '20180802', '20180803', '20180807', '20180817', '20181104',
        '20190820', '20191103'
        ],
    'CAISO': []
}
HOST = os.environ["PG_HOST"]
USER = os.environ["PG_USER"]
PWD = os.environ["PG_PWD"]
DB = "FRONTOFFICE"

engine = sqlalchemy.create_engine(f'postgresql+psycopg2://{USER}:{PWD}@{HOST}:5432/{DB}')

def get_market_id(market):
    conn = engine.connect()
    market_id = pd.read_sql(f"select market_id from new_model.market where market_name='{market.upper()}'", conn)['market_id'].values[0]
    conn.close()
    return int(market_id)

def get_missing_dart(market, lookback_days=None, hour_offset=1, output_type='list'):
    assert output_type in ('list', 'raw')
    if lookback_days==None:
        stdt = datetime.datetime(2017, 1, 1).date()
    else:
        stdt = datetime.datetime.now().date() - datetime.timedelta(days=lookback_days)
    market_id = get_market_id(market)

    # query = f"""    
    # select * from (
    #     select t1.opr_date, t2.opr_hour, count(da_lmp) da_count, count(rt_lmp) rt_count
    #     from new_model.operating_date t1 
    #     left join new_model.sn_dart_lmp t2
    #     on t2.opr_date = t1.opr_date 
    #     where t1.{market.lower()}_holiday = 0 and t2.market_id = {market_id} and t1.opr_date >= '{stdt}'
    #     group by t1.opr_date, t2.opr_hour
    # ) t
    # where t.da_count=0 or t.rt_count=0
    # """
    query = f"""    
    select t1.opr_date, t2.opr_hour, count(da_lmp) da_count, count(rt_lmp) rt_count
    from new_model.operating_date t1 
    left join new_model.sn_dart_lmp t2
    on t2.opr_date = t1.opr_date 
    where t1.{market}_holiday = 0 and t2.market_id = {market_id} and t1.opr_date >= '{stdt}'
    group by t1.opr_date, t2.opr_hour
    """
    conn = engine.connect()
    df = pd.read_sql(query, conn)
    conn.close()

    if output_type == 'raw':
        return df
    df['opr_date'] = pd.to_datetime(df['opr_date']).dt.date
    df['opr_datetime'] = df[['opr_date', 'opr_hour']].apply(lambda x: pd.to_datetime(x[0]) + datetime.timedelta(hours=x[1]), axis=1)
    # df['exception'] = df[~df['opr_date'].astype(str).apply(lambda x: x.replace('-', '')).isin(EXCEPTION_DATES.get(market, []))]
    
    cr1 = df['opr_datetime'] <= pd.to_datetime(datetime.datetime.today().date()) + datetime.timedelta(hours=datetime.datetime.now(pytz.timezone(TIMEZONES[market.upper()])).hour - hour_offset)
    cr2 = df['opr_date'] <= datetime.datetime.today().date() - ALLOWED_RT_LAST_DATE_DELTA[market.upper()]
    cr3 = df['opr_date'] <= datetime.datetime.today().date() + datetime.timedelta(days=1)

    df['opr_date'] = df['opr_date'].astype(str)
    missing_rt = df[(df['rt_count']==0) & cr1 & cr2][['opr_date', 'opr_hour']].drop_duplicates().values.tolist()
    missing_rt_days = df[(df['rt_count']==0) & cr1 & cr2][['opr_date']].drop_duplicates().shape[0]
    if datetime.datetime.now(pytz.timezone(TIMEZONES[market.upper()])).hour<=12:
        missing_da = df[(df['da_count']==0) & cr1][['opr_date', 'opr_hour']].drop_duplicates().values.tolist()
        missing_da_days = df[(df['da_count']==0) & cr1][['opr_date']].drop_duplicates().shape[0]
        skipped_dates = df[(df['da_count']==0)&(df['rt_count']==0) & cr1][['opr_date']].drop_duplicates().values.tolist()
    else:
        missing_da = df[(df['da_count']==0) & cr3][['opr_date', 'opr_hour']].drop_duplicates().values.tolist()
        missing_da_days = df[(df['da_count']==0) & cr3][['opr_date']].drop_duplicates().shape[0]
        skipped_dates = df[(df['da_count']==0)&(df['rt_count']==0) & cr3][['opr_date']].drop_duplicates().values.tolist()
    return {
        'market': market.lower(),
        'missing_rt': missing_rt,
        'missing_da': missing_da,
        'skipped_dates': skipped_dates,
        'skipped_dates_count': len(skipped_dates),
        'last_datetime_da': str(pd.to_datetime(df[(df['da_count'] != 0)]['opr_date'].dropna()).max()),
        'last_datetime_rt': str(df[df['rt_count']!=0]['opr_datetime'].dropna().max()),
        'last_blank_datetime_da': str(pd.to_datetime(df[df['da_count'] == 0]['opr_date'].dropna()).max()),
        'last_blank_datetime_rt': str(df[(df['rt_count'] == 0) & cr1 & cr2]['opr_datetime'].dropna().max()),
        'missing_rt_days': missing_rt_days,
        'missing_da_days': missing_da_days
    }


def get_missing_solar(market, lookback_days=None, hour_offset=1, output_type='list'):
    market = market.lower()
    assert output_type in ('list', 'raw')
    if lookback_days == None:
        stdt = datetime.datetime(2017, 1, 1).date()
    else:
        stdt = datetime.datetime.now().date() - datetime.timedelta(days=lookback_days)

    if market == 'ercot':
        indicator_act = 'actual_solar_gen'
        indicator_fct = 'stppf'
    if market == 'miso':
        indicator = None
    if market == 'isone':
        indicator = None
    if market == 'nyiso':
        indicator = None
    if market == 'caiso':
        indicator = None
    query = f"""
    select t1.opr_date, t1.opr_hour, count({indicator_act}) actu_count, count({indicator_fct}) fcst_count
    from (select tt1.opr_date, tt1.{market}_holiday, tt2.opr_hour from new_model.operating_date tt1 cross join new_model.operating_hour tt2) t1
    left join new_model.sn_{market}_actual_solar_fcst t2
    on t2.opr_date = t1.opr_date 
    where t1.{market}_holiday = 0 and t1.opr_date<= current_date + interval '1 day' and t1.opr_date > '{stdt}'
    group by t1.opr_date, t1.opr_hour 
    order by t1.opr_date desc, t1.opr_hour desc
    """

    conn = engine.connect()
    df = pd.read_sql(query, conn)
    conn.close()

    if output_type == 'raw':
        return df
    df['opr_date'] = pd.to_datetime(df['opr_date']).dt.date
    df['opr_datetime'] = df[['opr_date', 'opr_hour']].apply(
        lambda x: pd.to_datetime(x[0]) + datetime.timedelta(hours=x[1]), axis=1)

    return {
        "market": market.upper(),
        "missing_actual": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())][["opr_date", "opr_hour"]].drop_duplicates().values.tolist(),
        "missing_forecast": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))][["opr_date", "opr_hour"]].drop_duplicates().values.tolist(),
        "skipped_dates": df[(df["actu_count"] == 0) & (df["fcst_count"] == 0)]["opr_date"].drop_duplicates().values.tolist(),
        "skipped_dates_count": df[(df["actu_count"] == 0) & (df["fcst_count"] == 0)]["opr_date"].drop_duplicates().shape[0],
        "last_datetime_actual": df[(df["actu_count"] != 0)]['opr_datetime'].max(),
        "last_datetime_forecast": df[(df["fcst_count"] != 0)]['opr_datetime'].max(),
        "last_blank_datetime_actual": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())]['opr_datetime'].max(),
        "last_blank_datetime_forecast": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))]['opr_datetime'].max(),
        "missing_actual_days": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())]['opr_date'].unique().shape[0],
        "missing_forecast_days": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))]['opr_date'].unique().shape[0]
    }

def get_missing_load(market, lookback_days=None, hour_offset=1, output_type='list'):
    market = market.lower()
    assert output_type in ('list', 'raw')
    if lookback_days == None:
        stdt = datetime.datetime(2017, 1, 1).date()
    else:
        stdt = datetime.datetime.now().date() - datetime.timedelta(days=lookback_days)

    if market == 'ercot':
        indicator = 'north'
    if market == 'miso':
        indicator = None
    if market == 'isone':
        indicator = None
    if market == 'nyiso':
        indicator = None
    if market == 'caiso':
        indicator = None
    
    query = f"""
    select t1.opr_date, t1.opr_hour, count({indicator}_actual_load_mwh) actu_count, count({indicator}_fcst_load_mwh) fcst_count
    from (select tt1.opr_date, tt1.{market}_holiday, tt2.opr_hour from new_model.operating_date tt1 cross join new_model.operating_hour tt2) t1
    left join new_model.sn_{market}_actual_load_fcst t2
    on t2.opr_date = t1.opr_date 
    where t1.{market}_holiday = 0 and t1.opr_date<= current_date + interval '1 day' and t1.opr_date > '{stdt}'
    group by t1.opr_date, t1.opr_hour 
    order by t1.opr_date desc, t1.opr_hour desc
    """

    conn = engine.connect()
    df = pd.read_sql(query, conn)
    conn.close()

    if output_type == 'raw':
        return df
    df['opr_date'] = pd.to_datetime(df['opr_date']).dt.date
    df['opr_datetime'] = df[['opr_date', 'opr_hour']].apply(
        lambda x: pd.to_datetime(x[0]) + datetime.timedelta(hours=x[1]), axis=1)

    return {
        "market": market.upper(),
        "missing_actual": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())][["opr_date", "opr_hour"]].drop_duplicates().values.tolist(),
        "missing_forecast": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))][["opr_date", "opr_hour"]].drop_duplicates().values.tolist(),
        "skipped_dates": df[(df["actu_count"] == 0) & (df["fcst_count"] == 0)]["opr_date"].drop_duplicates().values.tolist(),
        "skipped_dates_count": df[(df["actu_count"] == 0) & (df["fcst_count"] == 0)]["opr_date"].drop_duplicates().shape[0],
        "last_datetime_actual": df[(df["actu_count"] != 0)]['opr_datetime'].max(),
        "last_datetime_forecast": df[(df["fcst_count"] != 0)]['opr_datetime'].max(),
        "last_blank_datetime_actual": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())]['opr_datetime'].max(),
        "last_blank_datetime_forecast": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))]['opr_datetime'].max(),
        "missing_actual_days": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())]['opr_date'].unique().shape[0],
        "missing_forecast_days": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))]['opr_date'].unique().shape[0]
    }

def get_missing_wind(market, lookback_days=None, hour_offset=1, output_type='list'):
    market = market.lower()
    assert output_type in ('list', 'raw')
    if lookback_days == None:
        stdt = datetime.datetime(2017, 1, 1).date()
    else:
        stdt = datetime.datetime.now().date() - datetime.timedelta(days=lookback_days)

    if market=='ercot':
        indicator = 'north'
    if market=='miso':
        indicator = 'north'
    if market=='isone':
        indicator = 'north'
    if market=='nyiso':
        indicator = 'north'
    if market=='caiso':
        indicator = 'north'
    query = f"""
    select t1.opr_date, t1.opr_hour, count({indicator}_actual) actu_count, count({indicator}_fcst) fcst_count
    from (select tt1.opr_date, tt1.{market}_holiday, tt2.opr_hour from new_model.operating_date tt1 cross join new_model.operating_hour tt2) t1
    left join new_model.sn_{market}_actual_wind_fcst t2
    on t2.opr_date = t1.opr_date 
    where t1.{market}_holiday = 0 and t1.opr_date<= current_date + interval '1 day' and t1.opr_date > '{stdt}'
    group by t1.opr_date, t1.opr_hour 
    order by t1.opr_date desc, t1.opr_hour desc
    """

    conn = engine.connect()
    df = pd.read_sql(query, conn)
    conn.close()

    if output_type == 'raw':
        return df
    df['opr_date'] = pd.to_datetime(df['opr_date']).dt.date
    df['opr_datetime'] = df[['opr_date', 'opr_hour']].apply(lambda x: pd.to_datetime(x[0]) + datetime.timedelta(hours=x[1]), axis=1)
    
    return {
        "market": market.upper(),
        "missing_actual": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())][["opr_date", "opr_hour"]].drop_duplicates().values.tolist(),
        "missing_forecast": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))][["opr_date", "opr_hour"]].drop_duplicates().values.tolist(),
        "skipped_dates": df[(df["actu_count"]==0)&(df["fcst_count"]==0)]["opr_date"].drop_duplicates().values.tolist(),
        "skipped_dates_count": df[(df["actu_count"] == 0) & (df["fcst_count"] == 0)]["opr_date"].drop_duplicates().shape[0],
        "last_datetime_actual": df[(df["actu_count"] != 0)]['opr_datetime'].max(),
        "last_datetime_forecast": df[(df["fcst_count"] != 0)]['opr_datetime'].max(),
        "last_blank_datetime_actual": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())]['opr_datetime'].max(),
        "last_blank_datetime_forecast": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))]['opr_datetime'].max(),
        "missing_actual_days": df[(df["actu_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date())]['opr_date'].unique().shape[0],
        "missing_forecast_days": df[(df["fcst_count"] == 0) & (df["opr_date"] <= datetime.datetime.now().date()+datetime.timedelta(days=1))]['opr_date'].unique().shape[0]
    }


if __name__ == '__main__':
    # print(get_missing_dart('NYISO'))
    pass
