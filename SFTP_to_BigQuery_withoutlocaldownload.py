## This code connects with client's vendor's FTP server, read file from that FTP Server
## via SFTP protocol without downloading locally, process data and stream to BigQuery.

"""Function called by PubSub trigger to execute cron job tasks."""
import datetime
import pandas as pd

import logging
from string import Template
import pandas_gbq
from google import oauth2
import json
import requests
import paramiko


def timecard_update(tableschema, req_file, sftp):

	f = sftp.open(req_file, 'r')
	timecard = pd.read_csv(f)
	timecard = timecard.loc[timecard['Device'].apply(lambda x: 'Total' not in x)]
	timecard.rename(columns={'Engine On': 'EngineON', 'Engine Off': 'EngineOFF'}, inplace=True)
	timecard['EngineON'] = timecard['Date'].astype('str')+ ' '+timecard['EngineON']
	timecard['EngineOFF'] = timecard['Date'].astype('str')+ ' '+timecard['EngineOFF']
	timecard['EngineON'] = timecard['EngineON'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %I:%M %p'))
	timecard['EngineOFF'] = timecard['EngineOFF'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%Y %I:%M %p'))
	timecard['Date'] = pd.to_datetime(timecard['Date'], format='%m/%d/%Y').dt.date

	
	logging.info('Sending timecard data to BQ!')
	pandas_gbq.to_gbq(timecard, destination_table='IntouchGPS.Timecard2',
							project_id='noted-casing-129313', if_exists='append', table_schema = tableschema,
							credentials=oauth2.service_account.Credentials.from_service_account_file('notedcasingbqcreds.json'))
	logging.info('Successfully Updated Timecard Table')

	return req_file

def readings_update(tableschema, req_file, sftp):
	
	f = sftp.open(req_file, 'r')
	readings = pd.read_csv(f)
	readings = readings.loc[readings['Device'].apply(lambda x: 'Device' not in x)]
	readings['When'] = readings['When'].apply(lambda x: pd.to_datetime(x, format='%m/%d/%y %I:%M %p'))
	readings['Day'] = readings['When'].dt.date
	
	logging.info('Sending Readings data to BQ!')
	pandas_gbq.to_gbq(readings, destination_table='IntouchGPS.Readings2',
					  project_id='noted-casing-129313', if_exists='append', table_schema=tableschema,
					  credentials=oauth2.service_account.Credentials.from_service_account_file('notedcasingbqcreds.json'))
	logging.info('Successfully Updated Readings Table')

	return req_file


def main(date, context):
	""" Caller Function which calls and execute queries from
			other functions"""
	try:
		current_time = datetime.datetime.utcnow()
		log_message = Template('Cloud Function was triggered on $time')
		logging.info(log_message.safe_substitute(time=current_time))

		host = "104.154.80.94"
		port = 22
		logging.info('connecting SFTP...')
		transport = paramiko.Transport(host,port)
		transport.connect(username="caarmo", password="cAARmO%4a3")
		sftp = paramiko.SFTPClient.from_transport(transport)
		sftp.chdir(path='upload')
		logging.info('SFTP connection successful!')


		## table schema of our big query table
		## stored in json, this is used to tell
		## by a function (appending data to table) to 
		## correctly append every column
		rfile = ""
		tfile = ""
		for each in sftp.listdir():
			if 'ReadingswithCoordinates' in each:
				req_file = each
				f = open('Readings_tableschema.json', 'r')
				tr = json.loads(f.read())
				f.close()

				rfile = readings_update(tr, req_file, sftp)
			if 'TimecardByDevice' in each:
				req_file2 = each

				f = open('Timecard_tableschema.json', 'r')
				tt = json.loads(f.read())
				f.close()

				tfile = timecard_update(tt, req_file2, sftp)

		if tfile:
			sftp.remove(tfile)

		if rfile:
			sftp.remove(rfile)

	except Exception as error:
		log_message = Template('$error').substitute(error=error)
		logging.error(log_message)

if __name__ == '__main__':
	main('date', 'context')
