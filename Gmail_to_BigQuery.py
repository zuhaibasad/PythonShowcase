## This code connects with Gmail, search for attachments with specific Subject text
## downloads that attachment, read, process and stream that into BigQuery data warehouse


"""Function called by PubSub trigger to execute cron job tasks."""
import datetime
import pandas as pd
from imap_tools import MailBox, AND
import logging
from string import Template
from google.cloud import bigquery
import pandas_gbq
from google import oauth2
import json

def email_to_bigquery(table_schema):
	""" This function gets todays attachement from servicepro email
		From there read file into dataframe and
		upload it to the bigquery."""

	logging.info('conneting Gmail ...')
	mailbox = MailBox('imap.gmail.com')
	mailbox.login('rohitk@caarmo.com', 'VER4#@#@ve44f', initial_folder='INBOX');
	logging.info('Gmail Login Successfully!')

	msg_received_date = datetime.datetime.today().date().strftime(format='%d-%b-%Y')

	data = ""
	filename = ""
	received_datetime = ""
	flag = True
	msgs = list(mailbox.fetch(AND('(FROM "dataexports@caarmo.com")', '(subject "Scheduled report on database aptive: CAARMO Daily Report")', 'ON '+msg_received_date ) ))

	if len(msgs) > 0:
		for msg in msgs:
			if flag:
				received_datetime = pd.to_datetime(msg.date, format='%Y-%m-%d %H:%M:%S%Z')
				xyz = msg.date
				for att in msg.attachments:
					filename = att.filename
					data = att.payload
				
				flag = False
			else:
				if received_datetime < pd.to_datetime(msg.date, format='%Y-%m-%d %H:%M:%S%Z'):
					received_datetime = pd.to_datetime(msg.date, format='%Y-%m-%d %H:%M:%S%Z')
					xyz = msg.date
					for att in msg.attachments:
						filename = att.filename
						data = att.payload

			geotab = pd.read_excel(data, skiprows=10)
			geotab.rename(columns={'Location.ZoneZoneTypes':'LocationZoneTypes','Location.ZoneExternalReference':'LocationZoneExternalReference'}, inplace=True)
			geotab['TripDetailIdlingDuration'] = geotab['TripDetailIdlingDuration'].apply(lambda x: round(x.minute+(x.second/60),2))
			geotab['TripDetailStopDuration'] = geotab['TripDetailStopDuration'].apply(lambda x: round(x.minute+(x.second/60),2))
			geotab['TripDetailDrivingDuraion'] = geotab['TripDetailDrivingDuraion'].apply(lambda x: round(x.minute+(x.second/60),2))
			logging.info('Sending dataframe to BigQuer table ...')
			pandas_gbq.to_gbq(geotab, destination_table='Aptive.Telematics',
								project_id='noted-casing-129313', if_exists='append', table_schema=table_schema,
							credentials=oauth2.service_account.Credentials.from_service_account_file('notedcasingbqcreds.json'))
		else:
			logging.info("No email found!")

def main(date, context):
	""" Caller Function which calls and execute queries from
			other functions"""
		
	## object of bigquery API regiestered with our google cloud
	## account credientials in file 'notedcasingbqcreds.json'
	## through this client we will make reuquest of query to big query table


	current_time = datetime.datetime.now()
	log_message = Template('Cloud Function was triggered on $time')
	logging.info(log_message.safe_substitute(time=current_time))
	## table schema of our big query table
	## stored in json, this is used to tell
	## by a function (appending data to table) to 
	## correctly append every column
	f = open('Geotab_tableschema.json')
	table_schema = json.loads(f.read())
	f.close()

	email_to_bigquery(table_schema)


if __name__ == '__main__':
	main('data', 'context')