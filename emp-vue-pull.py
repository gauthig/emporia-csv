#!/usr/bin/python3 
# -*- coding: utf-8 -*-

#### NOTE ####
# Please make sure your python binary is listed in the first line. 
# i.e. if your python is just python and not python 3 change it. 
# If your python is in /bin or /opt change it
# I know the above statement is normal shell programming, but I have recevied several questions on 'What does bad interpreter mean' 
#

# empimport.py
# Input - Emporia csv file
# Output - Inluxdb mesurements  (V1)
#

__author__ = 'Garrett Gauthier'
__copyright__ = 'Copyright 2022, Garrett Gauthier'
__author__ = 'Garrett Gauthier'
__copyright__ = 'Copyright 2022, Garrett Gauthier'
__credits__ = ['Garrett Gauthier', 'Others soon?']
__license__ = 'GPL'
__version__ = '1.0.1'
__VersionDate__ = '07/15/2022'
__maintainer__ = 'gauthig@github'
__github__ = 'https://github.com/gauthig/emporia-import'
__email__ = 'garrett-g@outlook.com'
__status__ = 'Dev'

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBClientError
from datetime import datetime
import datetime
import sys
import csv
import time
import argparse
import json
import pytz
from collections import defaultdict

prevarg = ''
influx_url = ''
input_file = ''
csvfileout = ''
dry_run = 'false'
verbose = 'false'
silent_run = 'false'
writer = ''
metricsout = []


def parseData(input_file, orgtimezone,  verbose):
    point = []
    rows_generated = 0
    dt_format = '%m/%d/%Y %H:%M:%S'
    parsed_row = ''
    with open(input_file, mode='r') as csv_file:
        csv_reader = csv.reader(csv_file,delimiter=',')
        energy_points = 0
        headers = next(csv_reader)
        devices = range(len(headers))
       # print( headers[0])
        for row in csv_reader:
            for i in devices:
                device_name = headers[i]
                device_name = device_name.replace(' (kWatts)', '') 
                device_name = device_name.strip()
                
                if i == 0:
                   continue
                dt_local = datetime.datetime.strptime(row[0], dt_format)
                dt_utc = dt_local.astimezone(pytz.UTC)
                dt_utc = dt_utc.strftime("%Y-%m-%d %H:%M:%S")
                dt_unix = datetime.datetime.strptime(dt_utc, "%Y-%m-%d %H:%M:%S")
                unixtime = time.mktime(dt_unix.timetuple())
                #parsed_row = "insert energy_usage,account_name=home,detailed=False,device_name=" + device_name + " usage="  + row[i] + " " + str(unixtime).rstrip('0').rstrip('.') + '0'
                #parsed_row = "insert energy_usage,account_name=home,detailed=False,device_name=" + device_name + " usage="  + row[i] + " " +  str(unixtime)

                point= {
                    "measurement": "energy_usage",
                    "tags": {
                        "account_name": "Home",
                        "device_name": device_name,
                        "detailed": "False",
                     },
                    "fields": {
                        "usage":  float(row[i]),
                    },
                    "time": dt_utc
                }

                metricsout.append(point)
                #print(parsed_row)
                rows_generated = rows_generated + 1
            
    #if verbose:
        # for key in columns:
        #     print(key,' :::: ', columns[key])
        
 
        
        
        
                
        #
        #        point = {
        #            "measurement" : "SCE",
        #            "tags": {"type": tag},
        #            "time": dt_utc,
        #            "fields": {"value": float(row[1]) * pmult}
        #            }
        #
        #        metricsout.append(point)

        #        if verbose:
        #            print (point)
 

    return rows_generated


def writedata():
    textout = ''
    current_date = datetime.datetime.now()
    fileout = 'energy' + str(int(current_date.strftime('%Y%m%d%H%M'
                               ))) + '.csv'

    #Influxformat
    with open(fileout, 'w', encoding='UTF-8') as f:
        csv_columns = 'measurement,time,value'
        
        #writer = csv.writer(f)
        f.write(csv_columns)
  
        for data in metricsout:
            textout = '\n' +  json.dumps(data)
            textout = textout.replace('{"measurement": "SCE", "tags": {"type": "', '')
            textout = textout.replace('"}, "time": "', ',')
            textout = textout.replace('", "fields": {"value":', ',')
            textout = textout.replace('}}', '')
            f.write(textout)
            print(textout)
            
            
            
    return ()


def senddata(
    hostname,
    port,
    user,
    password,
    dbname,
    batchsize,
    timezone,
    createdb,
    ):

    client = InfluxDBClient(hostname, port, user, password, dbname)

    if createdb == True:
        print ('Deleting database %s' % dbname)
        client.drop_database(dbname)
        print ('Creating database %s' % dbname)
        client.create_database(dbname)

    if len(metricsout) > 0:

        # print('Inserting %d metricsout...'%(len(metricsout)))

        client.switch_user(user, password)

        response = client.write_points(metricsout,batch_size=10000)
            #print("Wrote %d, response: %s" % (len(t), response))

    return ()


if __name__ == '__main__':
    parser = \
        argparse.ArgumentParser(description='Loads CSV file from emporia VUE energy and pushes it to influxdb.'
                                )
    parser.add_argument('--version', help='display version number',
                        action='store_true')
    parser.add_argument('-f', '--file', required=True,
                        help='*REQUIRED* filename of the utility provided csv kwh file')
    parser.add_argument('-n', '--hostname',
                        help='''the influxdb host name, no port or http
 example
 --host influxdb.mydomain.com''')
    parser.add_argument('-v', '--verbose',
                        help='verbose output - send copy of each line to stdout'
                        , action='store_true')
    parser.add_argument('-q', '--quiet',
                        help='do not print totals output',
                        action='store_true')
    parser.add_argument('-P', '--port',
                        help='port of the influxdb, if not provided it will default to 8086'
                        )
    parser.add_argument('-o', '--csvout',
                        help='sends parsed data to a csvfile in insert format  -p can be used or omitted with -o'
                        , action='store_true')
    parser.add_argument('-b', '--batchsize', type=int, default=5000,
                        help='Batch size. Default: 5000.')
    parser.add_argument('--dbname', nargs='?',
                        help='Database name.  Required if -n and -p used'
                        )
    parser.add_argument('-u', '--user', nargs='?',
                        help='influxdb userid')
    parser.add_argument('-p', '--password', nargs='?',
                        help='Influxdb password')
    parser.add_argument('-tz', '--timezone', default='UTC',
                        help='Timezone of supplied data. Default: UTC')
    parser.add_argument('--createdb', action='store_true',
                        default=False,
                        help='Drop database and create a new one.')
    args = parser.parse_args()

    if args.version:
        print ('emp-import.py - version', __version__)
        sys.exit()

    rows_generated = parseData(args.file,
            args.timezone,  args.verbose)

    if args.csvout:
        writedata()

    if args.hostname:
        senddata(
            args.hostname,
            args.port,
            args.user,
            args.password,
            args.dbname,
            args.batchsize,
            args.timezone,
            args.createdb,
            )

    if not args.quiet:
        print ('Import Complete')
        print ('Measurements Loaded : ', rows_generated)
      

exit