#!/usr/bin/python
__author__ = 'agoss'

import argparse
from csv import writer
from datetime import date, timedelta, datetime
import httplib2
import json
from os import path

from apiclient.discovery import build
from apiclient.errors import HttpError
import gen_utils
from oauth2client import client, file, tools
import psycopg2
import yaml

class struct:
    def __init__(self, **entries): 
        self.__dict__.update(entries)

SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')

home = path.expanduser("~")
CLIENT_SECRETS_PATH = path.expanduser("~") + '/.credentials/your_client_secret_file.json'
class SampledDataError(Exception): pass # stop the program when sampled data received

# configuration section
def init_config():
    global args
    argList = get_arg_list()
    return argList

def get_arg_list():
    parser = argparse.ArgumentParser(description='Parses command line arguments')
    parser.add_argument('--view_id', type=str, required=True, help= 'Unique GA view ID for retrieving analytics data')
	parser.add_argument('--web_property_id', type=str, help= 'The GA web property ID (UA-#########-#)')
    parser.add_argument('--profile_id', type=str, help='The GA profile ID (########)')
    parser.add_argument('--insert_table', type=str, required=True, help='PostgreSQL table where data will be inserted')
    parser.add_argument('--col_num', type=int, default=11, help='Number of columns in insert table')
    parser.add_argument('--date_start', type=str, default=str(datetime.today() - timedelta(days=1))[:10], help='Date to start pulling data from')
    parser.add_argument('--date_end', type=str, default=str(datetime.today() - timedelta(days=1))[:10], help='Date to stop pulling data from')
    return parser.parse_args()

def initialize_analyticsreporting():
  """Initializes the analyticsreporting service object.

  Returns:
    service an authorized analyticsreporting service object.
  """

  # Set up a Flow object to be used if we need to authenticate.
  flow = client.flow_from_clientsecrets(
      CLIENT_SECRETS_PATH, scope=SCOPES,
      message=tools.message_if_missing(CLIENT_SECRETS_PATH))

  # Prepare credentials, and authorize HTTP object with them.
  # If the credentials don't exist or are invalid run through the native client
  # flow. The Storage object will ensure that if successful the good
  # credentials will get written back to a file.
  storage = file.Storage('analyticsreporting.dat')
  credentials = storage.get()
  if credentials is None or credentials.invalid:
	flags = tools.argparser.parse_args('--auth_host_name localhost --logging_level INFO --noauth_local_webserver'.split())
    credentials = tools.run_flow(flow, storage, flags)
  http = credentials.authorize(http=httplib2.Http())

  # Build the service object.
  service = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
  return service

def get_results(service, pag_token, view_id, start, end, dim, met, fil):
  # Use the Analytics Service Object to query the Analytics Reporting API V4
  if fil['filters'][0] != 'none':
      return service.reports().batchGet(
        body={
            'reportRequests': [
            {
                'viewId': view_id,
                'dateRanges': [{'startDate': start, 'endDate': end}],
                'dimensions': dim,
                'dimensionFilterClauses': fil,
                'metrics': met,
                "samplingLevel": "LARGE",
                "pageSize": 5000,
                "pageToken": pag_token
            }]
        }
      ).execute()
  else:
      return service.reports().batchGet(
        body={
            'reportRequests': [
            {
                'viewId': view_id,
                'dateRanges': [{'startDate': start, 'endDate': end}],
                'dimensions': dim,
                'metrics': met,
                "samplingLevel": "LARGE",
                "pageSize": 5000,
                "pageToken": pag_token
            }]
        }
      ).execute()

def ContainsSampledData(report):
  """Determines if the report contains sampled data.

   Args:
       report (Report): An Analytics Reporting API V4 response report.

  Returns:
      bool: True if the report contains sampled data.
  """
  report_data = report.get('data', {})
  sample_sizes = report_data.get('samplesReadCounts', [])
  sample_spaces = report_data.get('samplingSpaceSizes', [])
  if sample_sizes and sample_spaces:
    return True
  else:
    return False

def value_string(insert_number):
    val_str = '%s'
    while insert_number > 1:
        val_str = val_str + ', %s'
        insert_number -= 1
    return val_str

def write_results(results, cur, conn, webpropertyid, profileid, table, col):
    # writing results to the Postgres table - appending in source if needed
    insert_date = str(datetime.now().date())

    for report in results.get('reports', []):
        sampled = ContainsSampledData(report)
        if sampled:
            # force an error if query returns data that is sampled
            print ('Error: Query contains sampled data!')
            raise SampledDataError
        rows = report.get('data', {}).get('rows', [])

        for row in rows:
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            dimensions[0] = datetime.strptime(str(dimensions[0]), '%Y%m%d').strftime("%Y-%m-%d") # convert to correct date value format
            insert_query= "insert into " + table + " VALUES ("+ value_string(col) + ")"

            for i, values in enumerate(dateRangeValues):
                insert_data = (insert_date, webpropertyid, profileid, dimensions[0], dimensions[1], values.get('values')[0], values.get('values')[1], values.get('values')[2], values.get('values')[3], values.get('values')[4], values.get('values')[5], values.get('values')[6]) # adjust per number of columns needed
                print insert_data
                
                try:
                    cur.execute(insert_query, insert_data)
                    conn.commit()

                except psycopg2.Error, e:
                    print 'line skipped: ' + str(e)
                    conn.rollback()
                    with open(dir + 'badLines_' + str(date.today())+ '.csv', 'a') as csvout:
                        outfile = writer(csvout, delimiter=',')
                        outfile.writerow(insert_data)

def main():
  args = init_config()
  service = initialize_analyticsreporting()

  with open(args.insert_table + '_config.json') as json_file: # assumes naming convention <<table_name>>_config.json
    gareportconfig = json.load(json_file)

  try:
        try:
			conn = psycopg2.connect(database=cfg.postgres['database'], user=cfg.postgres['username'], password=cfg.postgres['password'], host=cfg.postgres['host'], port=cfg.postgres['port'])
        except:
            raise
        else:
            print "Opened database successfully"

        cur = conn.cursor()

        pag_token = '0' # continuation token to get the next page of results, starts at 0
        results = get_results(service, pag_token, args.view_id, dateStart, dateEnd, gareportconfig["Dimensions"], gareportconfig["Metrics"], gareportconfig["dimensionFilterClauses"][0]) # query the core reporting API
        print 'Report date range: ' + dateStart + ' -> ' + dateEnd
        write_results(results, cur, conn, args.web_property_id, args.profile_id, args.insert_table, args.col_num)

        if 'rowCount' not in results['reports'][0]['data']:
            print 'report contains no data!'
        else: # use page tokens to paginate results
            if (int(pag_token) <= int(results['reports'][0]['data']['rowCount'])):
                pag_token = str(int(pag_token) + 5000) # page size is set to 5000, get next 5000 rows
                while (int(pag_token) <= int(results['reports'][0]['data']['rowCount'])):
                    results = get_results(service, pag_token, args.view_id, dateStart, dateEnd, gareportconfig["Dimensions"], gareportconfig["Metrics"], gareportconfig["Filters"])
                    write_results(results, cur, conn, args.web_property_id, args.profile_id, args.insert_table, args.col_num)
                    pag_token = str(int(pag_token) + 5000) # page size is set to 5000, get next 5000 rows

        cur.close()
        conn.close()

  except TypeError, error:
    # Handle errors in constructing a query.
        print ('There was an error in constructing your query : %s' % error)

  except HttpError, error:
    # Handle API errors.
        print ('There was an API error : %s : %s' % (error.resp.status, error._get_reason()))

  except client.AccessTokenRefreshError:
        print ("The credentials have been revoked or expired, please re-run the application to re-authorize")

  print '\n**********DONE**********\n'

if __name__ == '__main__':
  try:
    main()
  except:
    gen_utils.error_logging('main() handler exception:', str(path.basename(__file__)))
    raise