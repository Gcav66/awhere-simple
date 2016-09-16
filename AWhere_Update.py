# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

from oauthlib.oauth2 import BackendApplicationClient
from requests_oauthlib import OAuth2Session
from calendar import monthrange
import csv
from datetime import date, timedelta

key = ''
secret = ''

class AwhereUpdate(object):

    field_url = 'https://api.awhere.com/v2/fields'
    """
    "GET /v2/weather/locations/" +  row['latnum']+ "," + row['longnum'] + \
                                    "/observations/" + startdate + "," + enddate + "/?blockSize=1"
    """
    

    def __init__(self, key, secret):
        self.key = key.strip()
        self.secret = secret.strip()
        self.location_url = 'https://api.awhere.com/v2/weather/locations'
        self.pet_url = 'https://api.awhere.com/v2/agronomics/fields'


    def fetch_token(self):
        client = BackendApplicationClient(client_id = self.key)
        oauth = OAuth2Session(client=client)
        token = oauth.fetch_token(token_url='https://api.awhere.com/oauth/token', client_id=self.key, client_secret=self.secret)
        client = OAuth2Session(key, token=token)
        return client

    def single_call(self, mylat, mylong, startdate, enddate):

        url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/observations/" + \
                  str(startdate) + "," + str(enddate) + "/?limit=120"
        print url        
        client = self.fetch_token()
        result = client.get(url)
        return result.json()


    def flatten_single(self, results):
        obsvData = []
        for index, result in enumerate(results['observations']):
            myRow = {}
            myRow = {'date': result['date'],
                   'precipitation': result['precipitation']['amount'],
                   'solar': result['solar']['amount'],
                   'humid_max': result['relativeHumidity']['max'],
                   'humid_min': result['relativeHumidity']['min'],
                   'wind_avg': result['wind']['average'],
                   'temp_max': float(result['temperatures']['max']),
                   'temp_min': float(result['temperatures']['min']),
                   'latitude': result['location']['latitude'],
                   'longitude': result['location']['longitude'],
                   'id': str(index)
                  }
            obsvData.append(myRow)
        return obsvData

    def flatten_singles(self, obs_results):
        obsvData = []
        for results in obs_results:
            for index, result in enumerate(results['observations']):
              myRow = {}
              myRow = {'date': result['date'],
                       'precipitation': result['precipitation']['amount'],
                       'solar': result['solar']['amount'],
                       'humid_max': result['relativeHumidity']['max'],
                       'humid_min': result['relativeHumidity']['min'],
                       'wind_avg': result['wind']['average'],
                       'temp_max': float(result['temperatures']['max']),
                       'temp_min': float(result['temperatures']['min']),
                       'latitude': result['location']['latitude'],
                       'longitude': result['location']['longitude']
                      }
              obsvData.append(myRow)
        return obsvData

    def single_forecast(self, mylat, mylong, startdate='', enddate=''):

        #url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + \
                  #str(startdate) + "," + str(enddate) + "/?blockSize=1"
        if startdate and enddate:
            url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + \
                  str(startdate) + "," + str(enddate) + "/?blockSize=1"
        elif startdate:
            url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + \
                  str(startdate) + "/?blockSize=1"
        else:
            url = self.location_url + "/" + str(mylat) + "," + str(mylong) + "/forecasts/" + "?blockSize=1"
        print url        
        client = self.fetch_token()
        result = client.get(url)
        return result.json()

    def flatten_forecast(self, results):
        try:
            obsvData = []
            for result in results['forecast']:
                myRow = {}
                myRow = {'startTime': result['startTime'],
                         'endTime': result['endTime'],
                         'precipitation_units': result['precipitation']['units'],
                         'precipitation_chance': result['precipitation']['chance'],
                         'precipitation_amount': result['precipitation']['amount'],
                         'conditionsText': result['conditionsText'],
                         'wind_units': result['wind']['units'],
                         'wind_max': result['wind']['max'],
                         'wind_min': result['wind']['min'],
                         'wind_average': result['wind']['average'],
                         'relativeHumidity_max': result['relativeHumidity']['max'],
                         'relativeHumidity_average': result['relativeHumidity']['average'],
                         'relativeHumidity_min': result['relativeHumidity']['min'],
                         'solar_units': result['solar']['units'],
                         'solar_amount': result['solar']['amount'],
                         'dewPoint_units': result['dewPoint']['units'],
                         'dewPoint_units': result['dewPoint']['amount'],
                         'conditionsCode': result['conditionsCode'],
                         'sky_sunshine': result['sky']['sunshine'],
                         'sky_cloudCover': result['sky']['cloudCover'],
                         'temperature_unit': result['temperatures']['units'],
                         'temperature_max': result['temperatures']['max'],
                         'temperature_min': result['temperatures']['min']
                        }
                obsvData.append(myRow)
            return obsvData
        except KeyError:
            obsvData = []
            for day in results['forecasts']:
                for result in day['forecast']:
                    myRow = {}
                    myRow = {'startTime': result['startTime'],
                             'endTime': result['endTime'],
                             'precipitation_units': result['precipitation']['units'],
                             'precipitation_chance': result['precipitation']['chance'],
                             'precipitation_amount': result['precipitation']['amount'],
                             'conditionsText': result['conditionsText'],
                             'wind_units': result['wind']['units'],
                             'wind_max': result['wind']['max'],
                             'wind_min': result['wind']['min'],
                             'wind_average': result['wind']['average'],
                             'relativeHumidity_max': result['relativeHumidity']['max'],
                             'relativeHumidity_average': result['relativeHumidity']['average'],
                             'relativeHumidity_min': result['relativeHumidity']['min'],
                             'solar_units': result['solar']['units'],
                             'solar_amount': result['solar']['amount'],
                             'dewPoint_units': result['dewPoint']['units'],
                             'dewPoint_units': result['dewPoint']['amount'],
                             'conditionsCode': result['conditionsCode'],
                             'sky_sunshine': result['sky']['sunshine'],
                             'sky_cloudCover': result['sky']['cloudCover'],
                             'temperature_unit': result['temperatures']['units'],
                             'temperature_max': result['temperatures']['max'],
                             'temperature_min': result['temperatures']['min']
                            }
                    obsvData.append(myRow)
            return obsvData
    
    def flatten_batch(self, results):
        obsvData = []
        for result in results['results']:
            for obsv in result['payload']['observations']:
                myRow = {}
                try:
                    myRow = {'date': obsv['date'],
                             'precipitation': obsv['precipitation']['amount'],
                             'solar': obsv['solar']['amount'],
                             'humid_max': obsv['relativeHumidity']['max'],
                             'humid_min': obsv['relativeHumidity']['min'],
                             'wind_avg': obsv['wind']['average'],
                             'temp_max': float(obsv['temperatures']['max']),
                             'temp_min': float(obsv['temperatures']['min']),
                             'id': result['title'].split("_")[1]
                            }
                except TypeError:
                    myRow = {'date': obsv['date'],
                             'precipitation': obsv['precipitation']['amount'],
                             'solar': obsv['solar']['amount'],
                             'humid_max': obsv['relativeHumidity']['max'],
                             'humid_min': obsv['relativeHumidity']['min'],
                             'wind_avg': obsv['wind']['average'],
                             'temp_max': obsv['temperatures']['max'],
                             'temp_min': obsv['temperatures']['min'],
                             'id': result['title'].split("_")[1]
                            }
                obsvData.append(myRow)
        return obsvData

    def format_date(self, startDate, endDate):
        if len(startDate.split("-")[0]) == 4:
            year_0 = startDate.split("-")[0]
            month_0 = startDate.split("-")[1]
            day_0 = startDate.split("-")[2]
            year_1 = endDate.split("-")[0]
            month_1 = endDate.split("-")[1]
            day_1 = endDate.split("-")[2]
            startDate_str = str(month_0) + "-" + str(year_0) + "-" + str(day_0)
            endDate_str = str(month_1) + "-" + str(year_1) + "-" + str(day_1)
            startDate_date = date(int(year_0), int(month_0), int(day_0))
            endDate_date = date(int(year_1), int(month_1), int(day_1))
        else:
            month_0 = startDate.split("-")[0]
            year_0 = startDate.split("-")[1]
            day_0 = startDate.split("-")[2]
            month_1 = endDate.split("-")[0]
            year_1 = endDate.split("-")[1]
            day_1 = endDate.split("-")[2]
            startDate_str = startDate
            endDate_str = endDate
            startDate_date = date(int(year_0), int(month_0), int(day_0))
            endDate_date = date(int(year_1), int(month_1), int(day_1))
        return startDate_str, endDate_str, startDate_date, endDate_date

    def perdelta(self, start, end, delta):
        myDates = []
        curr = start
        while curr < end:
            #yield curr
            new_start = '{:%m}'.format(curr)+ "-" + '{:%Y}'.format(curr) + "-" + '{:%d}'.format(curr)
            myDates.append(new_start)
            curr += delta
        new_end = '{:%m}'.format(end)+ "-" + '{:%Y}'.format(end) + "-" + '{:%d}'.format(end)
        myDates.append(new_end)
        return myDates

    def build_pet_url(self, mylat, mylong, startDate, endDate):
        myUrls = []
        startDate_str, endDate_str, startDate_date, endDate_date = self.format_date(startDate, endDate)
        myDates = self.perdelta(startDate_date, endDate_date, timedelta(days=120))
        for i, date in enumerate(myDates):
            try:
                url = ('https://api.awhere.com/v2/agronomics/locations/'+ str(mylat) +
                "," + str(mylong) + '/agronomicvalues/' + str(date) + "," + str(myDates[i+1]))
                myUrls.append(url)
            except IndexError:
                continue
        return myUrls

    def build_obs_url(self, mylat, mylong, startDate, endDate):
        myUrls = []
        startDate_str, endDate_str, startDate_date, endDate_date = self.format_date(startDate, endDate)
        myDates = self.perdelta(startDate_date, endDate_date, timedelta(days=120))
        for i, date in enumerate(myDates):
            try:
                url = (self.location_url + "/" + str(mylat) +
                "," + str(mylong) + '/observations/' + str(date) + "," + str(myDates[i+1]))
                myUrls.append(url)
            except IndexError:
                continue
        return myUrls

    def make_pet_call(self, myUrls):
        results = []
        for url in myUrls:
            client = self.fetch_token()
            result = client.get(url)
            results.append(result.json())
        return results


    def get_pet(self, mylat, mylong, startDate, endDate):
        if len(startDate.split("-")[0]) == 4:
            year_0 = startDate.split("-")[0]
            month_0 = startDate.split("-")[1]
            day_0 = startDate.split("-")[2]
            year_1 = endDate.split("-")[0]
            month_1 = endDate.split("-")[1]
            day_1 = endDate.split("-")[2]
            startDate = str(month_0) + "-" + str(year_0) + "-" + str(day_0)
            endDate = str(month_1) + "-" + str(year_1) + "-" + str(day_1)
        url = 'https://api.awhere.com/v2/agronomics/locations/'+ str(mylat) + "," + \
               str(mylong) + '/agronomicvalues/' + str(startDate) + "," + str(endDate)
        print url
        client = self.fetch_token()

        result = client.get(url)
        return result.json()

    def flatten_pets(self, pet_results):
        myData = []
        try:
            for pet_result in pet_results:
                for result in pet_result['dailyValues']:
                    try:
                        myRow = {'pet': result['pet']['amount'],
                                 'gdd': result['gdd'],
                                 'ppet': result['ppet'],
                                 'units': 'mm',
                                 'date': result['date'],
                                 'latitude': pet_result['location']['latitude'],
                                 'longitude': pet_result['location']['longitude']}
                    except TypeError:
                        myRow = {'pet': result['pet'],
                                 'gdd': result['gdd'],
                                 'ppet': result['ppet'],
                                 'units': 'mm',
                                 'date': result['date'],
                                 'latitude': pet_result['location']['latitude'],
                                 'longitude': pet_result['location']['longitude']}
                    myData.append(myRow)
            return myData
        except KeyError:
            print pet_result
            return pet_result
        
    def flatten_pet(self, pet_result):
        myData = []
        try:
            for result in pet_result['dailyValues']:
                try:
                    myRow = {'pet': result['pet']['amount'],
                             'gdd': result['gdd'],
                             'ppet': result['ppet'],
                             'units': 'mm',
                             'date': result['date'],
                             'latitude': pet_result['location']['latitude'],
                             'longitude': pet_result['location']['longitude']}
                except TypeError:
                    myRow = {'pet': result['pet'],
                             'gdd': result['gdd'],
                             'ppet': result['ppet'],
                             'units': 'mm',
                             'date': result['date'],
                             'latitude': pet_result['location']['latitude'],
                             'longitude': pet_result['location']['longitude']}
                myData.append(myRow)
            return myData
        except KeyError:
            print pet_result
            return pet_result
"""        
def create_batch(myfile, start_year, end_year):
    myData = {}
    with open(myfile) as f:
        latlong = csv.DictReader(f)
        for row in latlong:
            myName = "id_" + row['anonymous_id'] + "_weather_data"
            myId = []
            for z in range(start_year, end_year):
                for i in range(1, 13):
                #for i in range(1, 5):
                    myCall = {}
                    day = monthrange(z, i)[1]
                    if len(str(i)) < 2:
                        startdate = str(z) + '-0' + str(i) + '-01'
                        enddate = str(z) + '-0' + str(i) + '-' + str(day)
                    else:
                        startdate = str(z) + '-' + str(i) + '-01'
                        enddate = str(z) + '-' + str(i) + '-' + str(day)
                    
                    myCall["title"] = myName + "_" + str(z) + "_" + str(i)
                    myCall["api"] = "GET /v2/weather/locations/" +  row['latnum']+ "," + row['longnum'] + \
                                    "/observations/" + startdate + "," + enddate + "/?blockSize=1"
                    myId.append(myCall)
                    #counter += 1
            myData[row['anonymous_id']] = myId
            
    return myData
    

def make_call(get_requests, output_file):
    calls_made = []    
    myIDs = []
    with open(output_file, "wb") as w:
        for k, v in myData.iteritems():
            mypayload = {
                        "title":output_file.strip("txt"),
                         "type":"batch",
                         "requests": v
                        }
            client = fetch_token(key, secret)
            stuff = client.post(r'https://api.awhere.com/v2/jobs',json=mypayload)
    
            myIDs.append(stuff.json()['jobId'])
    
            w.write(str(stuff.json()['jobId']) + "\n")
            print "completed " + str(stuff.json()['jobId'])
            calls_made.append(k)
    return calls_made

myData = create_batch("C:\\Users\\gus\\workspace\\awhere\\Sofia_Data\\NG_1990_anonymized.csv", 1980, 1991)

print myData['2'][0]

myCalls = make_call(myData, "C:\\Users\\gus\\workspace\\awhere\\app_v2\\ng_1990_ids.txt")

print len(myCalls)
print myCalls[0]
"""