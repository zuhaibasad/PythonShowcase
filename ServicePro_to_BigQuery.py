### This code collects data from one of Client's vendor and stored it into BigQuery

"""Function called by PubSub trigger to execute cron job tasks."""
import datetime
import pandas as pd
import logging
from string import Template
from google.cloud import bigquery
import pandas_gbq
from google import oauth2
import json

def appointments_to_bigquery():
	""" This function gets todays attachement from servicepro email
		From there read file into dataframe and
		upload it to the bigquery."""

	start = '2020-12-31'
	end = str((datetime.datetime.utcnow() - datetime.timedelta(hours=5)).date())
	url = 'https://pickettpest.pestroutes.com/api/appointment/search?dateAdded={"operator":"BETWEEN","value":["' + start + '", "' + end + '"]}&authenticationKey=fdb1984acc96a5e3df5bfd51c52dac54a18c920274790f1973b1eba33c5e5ba3&authenticationToken=5155d78d3a45fe03e0b5c4443e23b51a83e43d7a3654aa0773ef2be34b8d6113'
	
	payload = "{}"
	
	logging.info("Getting Appointment_IDs")
	response = requests.request("POST", url, data=payload)
	data = json.loads(response.text)
	appointment_ids = list(data['appointmentIDs'])
	logging.info("Received Appointment_IDs of length ",len(appointment_ids))

	for i in range(0,len(appointment_ids),200):
		if (i + 200) > (len(appointment_ids)-1):
			param = {'appointmentIDs[]': appointment_ids[i:]}
		else:
			param = {'appointmentIDs[]': appointment_ids[i:i+200]}
		
		url = "https://pickettpest.pestroutes.com/api/appointment/get?authenticationKey=fdb1984acc96a5e3df5bfd51c52dac54a18c920274790f1973b1eba33c5e5ba3&authenticationToken=5155d78d3a45fe03e0b5c4443e23b51a83e43d7a3654aa0773ef2be34b8d6113"
		response = requests.request("POST", url, params=param)
		appointment_info = json.loads(response.text)
		appointment_info = appointment_info['appointments']
		df = pd.DataFrame(appointment_info)
		#df = df[['appointmentID', 'officeID', 'customerID', 'routeID', 'spotID', 'ticketID', 'date', 'start', 'end', 'statusText', 'isInitial', 'servicedBy', 'timeIn', 'timeOut', 'amountCollected', 'type', 'dateAdded']]
		#df = df.rename(columns={'statusText': 'appointmentStatus', 'servicedBy': 'servicedById', 'timeIn': 'checkIn', 'timeOut': 'checkOut'})
		pandas_gbq.to_gbq(df, destination_table='geocode_converted_address.pest_appointments_new',
								project_id='noted-casing-129313', if_exists='append',
								credentials=oauth2.service_account.Credentials.from_service_account_file('notedcasingbqcreds.json'))

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

	appointments_to_bigquery()


if __name__ == '__main__':
	main('data', 'context')