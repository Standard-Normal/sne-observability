from flask import Flask, json, request, jsonify
import requests
import datetime
import os
import json
import pandas as pd
from pretty_html_table import build_table

import email_utils as eu
import iso_utils as iu


app = Flask(__name__)

def get_lookback_days():
    return (datetime.datetime.today().date() - datetime.datetime(2017, 1, 1).date()).days

@app.route('/ping')
def ping():
    """
    this is just an sample api that prints the health status of the app
    """
    t = datetime.datetime.now()
    response = {'time': str(t), 'status': 'ok', 'app': 'sne-observability system'}
    print('--->', response)
    return jsonify(response)


@app.route('/check_iso_solar/<iso>', methods=['GET'])
@app.route('/check_iso_solar/<iso>/<int:lookback>', methods=['GET'])
@app.route('/check_iso_solar/<iso>/<int:lookback>/<int:hour_offset>', methods=['GET'])
def check_iso_solar(iso, lookback=get_lookback_days(), hour_offset=1):
    r = iu.get_missing_solar(iso, lookback_days=lookback, hour_offset=hour_offset)
    if r['missing_actual'] != [] or r['missing_forecast'] != [] or r['skipped_dates']:
        r['status'] = 'red'
    else:
        r['status'] = 'green'

    if r['status'] != 'green':
        subject = f"SOLARDATA-COMPLETENESS-REDFLAG: {r['market']}"
        summary = pd.DataFrame(pd.Series({
            'REPORT TIME': str(datetime.datetime.now().replace(microsecond=0)),
            'MARKET': r['market'],
            'STATUS': r['status'].upper(),
            'ACTUAL LAST BLANK DATETIME': r['last_blank_datetime_actual'],
            'FORECAST LAST BLANK DATETIME': r['last_blank_datetime_forecast'],
            'ACTUAL LAST DATETIME': r['last_datetime_actual'],
            'FORECAST LAST DATETIME': r['last_datetime_forecast'],
            'SKIPPED DATES COUNT': len(r['skipped_dates']),
            'NUM DAYS W/ MISSING ACTUAL': r['missing_actual_days'],
            'NUM DAYS W/ MISSING FORECAST': r['missing_forecast_days'],
        }), columns=[''])
        body = f"""
        <h2>SUMMARY</h2>
            {build_table(summary, 'blue_light', font_size=9, index=True, width='100%')}
        """
        if len(r['skipped_dates']) > 0:
            body += f"""\n
            <h2>SKIPPED DATES</h2>
                {build_table(pd.DataFrame(r['skipped_dates'], columns=['OPR_DATE']), 'blue_light', font_size=9)}
            """
        if len(r['missing_actual']) > 0:
            body += f"""
            <h2>ACTUAL MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_actual'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        if len(r['missing_forecast']) > 0:
            body += f"""
            <h2>FORECAST MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_forecast'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        for email in eu.EMAILS:
            eu.send_flag_report(email, body, subject)
    return r

@app.route('/check_iso_load/<iso>', methods=['GET'])
@app.route('/check_iso_load/<iso>/<int:lookback>', methods=['GET'])
@app.route('/check_iso_load/<iso>/<int:lookback>/<int:hour_offset>', methods=['GET'])
def check_iso_load(iso, lookback=get_lookback_days(), hour_offset=1):
    r = iu.get_missing_load(iso, lookback_days=lookback, hour_offset=hour_offset)
    if r['missing_actual']!=[] or r['missing_forecast']!=[] or r['skipped_dates']:
        r['status'] = 'red'
    else:
        r['status'] = 'green'
    
    if r['status'] != 'green':
        subject = f"LOADDATA-COMPLETENESS-REDFLAG: {r['market']}"
        summary = pd.DataFrame(pd.Series({
            'REPORT TIME': str(datetime.datetime.now().replace(microsecond=0)),
            'MARKET': r['market'],
            'STATUS': r['status'].upper(),
            'ACTUAL LAST BLANK DATETIME': r['last_blank_datetime_actual'],
            'FORECAST LAST BLANK DATETIME': r['last_blank_datetime_forecast'],
            'ACTUAL LAST DATETIME': r['last_datetime_actual'],
            'FORECAST LAST DATETIME': r['last_datetime_forecast'],
            'SKIPPED DATES COUNT': len(r['skipped_dates']),
            'NUM DAYS W/ MISSING ACTUAL': r['missing_actual_days'],
            'NUM DAYS W/ MISSING FORECAST': r['missing_forecast_days'],
        }), columns=[''])
        body = f"""
        <h2>SUMMARY</h2>
            {build_table(summary, 'blue_light', font_size=9, index=True, width='100%')}
        """
        if len(r['skipped_dates']) > 0:
            body += f"""\n
            <h2>SKIPPED DATES</h2>
                {build_table(pd.DataFrame(r['skipped_dates'], columns=['OPR_DATE']), 'blue_light', font_size=9)}
            """
        if len(r['missing_actual']) > 0:
            body += f"""
            <h2>ACTUAL MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_actual'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        if len(r['missing_forecast']) > 0:
            body += f"""
            <h2>FORECAST MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_forecast'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        for email in eu.EMAILS:
            eu.send_flag_report(email, body, subject)
    return r

@app.route('/check_iso_wind/<iso>', methods=['GET'])
@app.route('/check_iso_wind/<iso>/<int:lookback>', methods=['GET'])
@app.route('/check_iso_wind/<iso>/<int:lookback>/<int:hour_offset>', methods=['GET'])
def check_iso_wind(iso, lookback=get_lookback_days(), hour_offset=1):
    assert iso.upper() in iu.ISOs
    assert isinstance(lookback, int)
    r = iu.get_missing_wind(iso, lookback_days=lookback, hour_offset=hour_offset)
    if r['missing_actual']!=[] or r['missing_forecast']!=[] or r['skipped_dates']:
        r['status'] = 'red'
    else:
        r['status'] = 'green'
    
    if r['status'] != 'green':
        subject = f"WINDDATA-COMPLETENESS-REDFLAG: {r['market']}"
        summary = pd.DataFrame(pd.Series({
            'REPORT TIME': str(datetime.datetime.now().replace(microsecond=0)),
            'MARKET': r['market'],
            'STATUS': r['status'].upper(),
            'ACTUAL LAST BLANK DATETIME': r['last_blank_datetime_actual'],
            'FORECAST LAST BLANK DATETIME': r['last_blank_datetime_forecast'],
            'ACTUAL LAST DATETIME': r['last_datetime_actual'],
            'FORECAST LAST DATETIME': r['last_datetime_forecast'],
            'SKIPPED DATES COUNT': len(r['skipped_dates']),
            'NUM DAYS W/ MISSING ACTUAL': r['missing_actual_days'],
            'NUM DAYS W/ MISSING FORECAST': r['missing_forecast_days'],
        }), columns=[''])
        body = f"""
        <h2>SUMMARY</h2>
            {build_table(summary, 'blue_light', font_size=9, index=True, width='100%')}
        """
        if len(r['skipped_dates']) > 0:
            body += f"""\n
            <h2>SKIPPED DATES</h2>
                {build_table(pd.DataFrame(r['skipped_dates'], columns=['OPR_DATE']), 'blue_light', font_size=9)}
            """
        if len(r['missing_actual']) > 0:
            body += f"""
            <h2>ACTUAL MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_actual'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        if len(r['missing_forecast']) > 0:
            body += f"""
            <h2>FORECAST MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_forecast'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        for email in eu.EMAILS:
            eu.send_flag_report(email, body, subject)
    return r


@app.route('/check_iso_dart/<iso>', methods=['GET'])
@app.route('/check_iso_dart/<iso>/<int:lookback>', methods=['GET'])
@app.route('/check_iso_dart/<iso>/<int:lookback>/<int:hour_offset>', methods=['GET'])
def check_iso_dart(iso, lookback=get_lookback_days(), hour_offset=1):
    """
    check and send alerts for missing da and/or rt
    return
    - market 
    - status: can be red, yellow, green for respectively alert, warning and nothing
    - details
    """
    assert iso.upper() in iu.ISOs
    assert isinstance(lookback, int)

    r = iu.get_missing_dart(iso, lookback_days=lookback, hour_offset=hour_offset)
    if r['missing_rt']!=[] or r['missing_da']!=[] or r['skipped_dates']:
        r['status'] = 'red'
    else:
        r['status'] = 'green'
    r['market'] = iso.upper()
    r['type'] = 'dart'

    if r['status']!='green':
        subject = f"LMPDATA-COMPLETENESS-REDFLAG: {r['market']}"
        summary = pd.DataFrame(pd.Series({
            'REPORT TIME': str(datetime.datetime.now().replace(microsecond=0)),
            'MARKET': r['market'],
            'STATUS': r['status'].upper(),
            'DA LAST BLANK DATETIME': r['last_blank_datetime_da'],
            'RT LAST BLANK DATETIME': r['last_blank_datetime_rt'],
            'DA LAST DATETIME': r['last_datetime_da'],
            'RT LAST DATETIME': r['last_datetime_rt'],
            'SKIPPED DATES COUNT': len(r['skipped_dates']),
            'NUM DAYS W/ MISSING DA': r['missing_da_days'],
            'NUM DAYS W/ MISSING RT': r['missing_rt_days'],
        }), columns=[''])
        body = f"""
        <h2>SUMMARY</h2>
            {build_table(summary, 'blue_light', font_size=9, index=True, width='100%')}
        """
        if len(r['skipped_dates'])>0:
            body += f"""\n
            <h2>SKIPPED DATES</h2>
                {build_table(pd.DataFrame(r['skipped_dates'], columns=['OPR_DATE']), 'blue_light', font_size=9)}
            """
        if len(r['missing_da'])>0:
            body += f"""
            <h2>DA MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_da'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        if len(r['missing_rt'])>0:
            body += f"""
            <h2>RT MISSING DATETIMES</h2>
                {build_table(pd.DataFrame(r['missing_rt'], columns=['OPR_DATE', 'OPR_HOUR']), 'blue_light', font_size=9)}
            """
        for email in eu.EMAILS:
            eu.send_flag_report(email, body, subject)
    return r


if __name__ == "__main__":
    #gcloud config set run/region us-west1
    app.run(host='0.0.0.0', port=8080, debug=True)
