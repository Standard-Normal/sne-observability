from flask import Flask, json, request, jsonify
import requests
import datetime as dt
import os

import email_utils as eu
import iso_utils as iu


app = Flask(__name__)

ISOs = ['ERCOT', 'MISO', 'NYISO', 'ISONE', 'CAISSO']


@app.route('/ping')
def ping():
    """
    this is just an sample api that prints the health status of the app
    """
    t = dt.datetime.now()
    response = {'time': str(t), 'status': 'ok', 'app': 'sne-observability system'}
    print('--->', response)
    return jsonify(response)


@app.route('/check_iso_dart/<iso>', methods=['GET'])
def check_iso_dart(iso):
    """
    check and send alerts for missing da and/or rt
    return
    - market 
    - status: can be red, yellow, green for respectively alert, warning and nothing
    - message
    """
    assert iso.upper() in ISOs
    res = {
        'market': iso.upper(),
        'status': 'green',
        'message': 'message here',
        'type': 'dart'
    }
    return res


@app.route('/check_iso_variables/<iso>', methods=['GET'])
def check_iso_variables(iso):
    """
    check and send alerts for missing
        - load
        - wind
        - solar
    return
        - market 
        - status: can be red, yellow, green for respectively alert, warning and nothing
        - message
    """
    assert iso.upper() in ISOs
    res = {
        'market': iso.upper(),
        'status': 'green',
        'message': 'message here',
        'type': 'variables'
    }
    return res

if __name__ == "__main__":
    #gcloud config set run/region us-west1
    app.run(host='0.0.0.0', port=8080, debug=True)
