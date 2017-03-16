from flask import Flask, request, redirect, render_template, send_file
from AWhere_Update import AwhereUpdate
import pandas as pd
import os


def create_app():
    app=Flask(__name__)
    app.config['PROPAGATE_EXCEPTIONS'] = True
    return app

app = create_app()

#with open("test_credentials.txt") as f:
    #data = f.read().split("\n")
    #key = data[0].strip()
    #secret = data[1].strip()
    


def get_single_data(akey, asec, mlat, mlong, sdate, edate):
    client = AwhereUpdate(akey, asec)
    #response = client.single_call(mlat, mlong, sdate, edate)
    obs_urls = client.build_obs_url(mlat, mlong, sdate, edate)
    obs_results = client.make_pet_call(obs_urls)
    flat = client.flatten_singles(obs_results)
    print type(flat)
    df = pd.DataFrame(flat)
    order = ['date', 'latitude', 'longitude', 'temp_max', 'temp_min',
    'precipitation', 'wind_avg', 'humid_max', 'humid_min', 'solar']
    ordered_df = df[order].drop_duplicates()
    xls_name = "GDA_AWHERE_Weather.xlsx"
    df_xls = ordered_df.to_excel(xls_name, index=False)
    return xls_name

def get_forecast(akey, asec, mlat, mlong, sdate, edate):
    client = AwhereUpdate(akey, asec)
    response = client.single_forecast(mlat, mlong, sdate, edate)
    flat = client.flatten_forecast(response)
    df = pd.DataFrame(flat)
    forecast_order = ['startTime', 'endTime', 'temperature_max', 'temperature_min',
                  'temperature_unit', 'precipitation_amount', 'precipitation_chance',
                  'precipitation_units', 'precipitation_chance', 'wind_average',
                  'wind_max', 'wind_min', 'wind_units', 'sky_sunshine', 'solar_amount',
                  'solar_units']
    ordered_df = df[forecast_order]
    xls_name = "GDA_AWHERE_Forecast_Weather.xlsx"
    df_xls = ordered_df.to_excel(xls_name, index=False)
    return xls_name

def get_forecast_simple(akey, asec, mlat, mlong):
    client = AwhereUpdate(akey, asec)
    response = client.single_forecast(mlat, mlong)
    flat = client.flatten_forecast(response)
    df = pd.DataFrame(flat)
    forecast_order = ['startTime', 'endTime', 'temperature_max', 'temperature_min',
                  'temperature_unit', 'precipitation_amount', 'precipitation_chance',
                  'precipitation_units', 'precipitation_chance', 'wind_average',
                  'wind_max', 'wind_min', 'wind_units', 'sky_sunshine', 'solar_amount',
                  'solar_units']
    ordered_df = df[forecast_order]
    xls_name = "GDA_AWHERE_Forecast_Weather.xlsx"
    df_xls = ordered_df.to_excel(xls_name, index=False)
    return xls_name

def get_pet(akey, asec, mlat, mlong, sdate, edate):
    client = AwhereUpdate(akey, asec)
    #response = client.get_pet(mlat, mlong, sdate, edate)
    #flat = client.flatten_pet(response)
    pet_urls = client.build_pet_url(mlat, mlong, sdate, edate)
    pet_results = client.make_pet_call(pet_urls)
    flat = client.flatten_pets(pet_results)
    try:
        df = pd.DataFrame(flat)
    except ValueError:
        return flat
    pet_order = ['date', 'latitude', 'longitude', 'gdd', 'pet', 'ppet', 'units']
    ordered_df = df[pet_order].drop_duplicates()
    xls_name = "GDA_AWHERE_PET_" + mlat + "_" + mlong + "_" + sdate + "_" + edate +".xlsx"
    df_xls = ordered_df.to_excel(xls_name, index=False)
    return xls_name

"""
@app.route("/", methods=['GET', 'POST'])
def upload():
    template = 'upload_file.html'
    if request.method == 'POST':
        if request.form['btn'] == 'observation':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            edate = request.form['end_date']
            xls_name = get_single_data(akey, asec, mlat, mlong, sdate, edate)
            if xls_name <> "GDA_AWHERE_Weather.xlsx":
                return render_template(template, error_message=xls_name)
            else:
                return send_file(xls_name, as_attachment=True)
        if request.form['btn'] == 'forecast':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            edate = request.form['end_date']
            xls_name = get_forecast(akey, asec, mlat, mlong, sdate, edate)
            return send_file(xls_name, as_attachment=True)
        if request.form['btn'] == 'pet':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            print sdate
            edate = request.form['end_date']
            print edate
            xls_name = get_pet(akey, asec, mlat, mlong, sdate, edate)
            if ".xlsx" not in xls_name:
                return xls_name['detailedMessage']
            return send_file(xls_name, as_attachment=True)
    return render_template(template)
"""
@app.route("/", methods=["GET", "POST"])
def index():
    return render_template("home.html")

@app.route("/observations", methods=["GET", "POST"])
def observations():
    if request.method == 'POST':
        if request.form['btn'] == 'observation':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            edate = request.form['end_date']
            xls_name = get_single_data(akey, asec, mlat, mlong, sdate, edate)
            if xls_name <> "GDA_AWHERE_Weather.xlsx":
                return render_template(template, error_message=xls_name)
            else:
                return send_file(xls_name, as_attachment=True)
        if request.form['btn'] == 'pet':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            sdate = request.form['start_date']
            print sdate
            edate = request.form['end_date']
            print edate
            xls_name = get_pet(akey, asec, mlat, mlong, sdate, edate)
            if ".xlsx" not in xls_name:
                return xls_name['detailedMessage']
            return send_file(xls_name, as_attachment=True)
    return render_template("observations.html")

@app.route("/forecasts", methods=["GET", "POST"])
def forecasts():
    if request.method == 'POST':
        if request.form['btn'] == 'forecast':
            akey = request.form['api_key']
            asec = request.form['api_secret']
            mlat = request.form['latitude_input']
            mlong = request.form['longitude_input']
            #sdate = request.form['start_date']
            #edate = request.form['end_date']
            xls_name = get_forecast_simple(akey, asec, mlat, mlong)#, sdate, edate)
            return send_file(xls_name, as_attachment=True)
    return render_template("forecasts.html")

@app.route("/bulk", methods=["GET", "POST"])
def bulk():
    return render_template("bulk.html")

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=int(port), debug=True)
