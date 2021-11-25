## This script collects data from tracking devices, other databases, process it, 
## integrate APIs, use Google APIs and then uploads to BigQuery efficiently to keep
## Cloud Costs low.

"""Function called by PubSub trigger to execute cron job tasks."""
import datetime
import herepy ## here API for python
import pandas as pd
import numpy as np
import math

## mpu library for haversin_distance calulation
from mpu import haversine_distance 
import logging
from string import Template
from google.cloud import bigquery
import pandas_gbq
from google import oauth2
import json
import requests

def get_streetaddress(lat, longi, address_dict, gc_reverseAPI):
  """ this function takes latitude and longitude and converts it into
      corressponding street address using HERE API
      Args: lat - latitude
          : longi - longitude
          : address_dict: dictionary containing some address alread present
                          in AddressDB table in big query
          : gc_reverseAPI : object of HERE API to convert geocoordinates
  """

  lat = str(lat)
  longi = str(longi)
  ## if latitude and longitude are not present
  ## or cached already then sends requests to the HERE API
  ## else ignore

  if (lat+","+longi) not in address_dict.keys():
    try:
      response = gc_reverseAPI.retrieve_addresses([lat, longi])
    except Exception as e:
      logging.error("Exception !!! %s for Geocoord %s, %s", e, lat, longi)
    else:
      resp_dict = response.as_dict()['Response']['View']    
      if resp_dict:
        address_dict[lat+","+longi] = resp_dict[0]['Result'][0]['Location']['Address']['Label']
      else:  
        address_dict[lat+","+longi] = "Not Available"
        logging.info('Could not convert to Street Address')

  return address_dict

def addressforevent(gc_reverseAPI, each, triptrack, address_dict):
	""" this function sets keys for triptrack and populate
			street_addresses in it
			Args: gc_reverseAPI - HERE API object for reverse geo coordinates
					: each - one row of big query table
					: triptrack - empty dictionary
					: address_dict - dictionary containing Street Addresses
			"""
	
	## gets the street address for latitude and longitude using
	## function get_streetaddress and populates it into address_dict
	address_dict = get_streetaddress(each['latitude'], each['longitude'], address_dict, gc_reverseAPI)
	street_address = address_dict[each['latitude']+","+each['longitude']]

	## is current row deviceID is not present in triptrack
	if each['deviceidentification'] not in triptrack:
		## then adds a new key of deviceID and other keys
		triptrack[each['deviceidentification']] = {each['tripnumber']: {each['eventtype']: { (each['latitude']+","+each['longitude']) : {'street_address': street_address} }}}
	else:
		## if deviceID already present, then check if tripnumber is there
		if each['tripnumber'] not in triptrack[each['deviceidentification']]:
			## not there already, then adds a new key of tripnumber and other keys
			triptrack[each['deviceidentification']][each['tripnumber']] = {each['eventtype']: { (each['latitude']+","+each['longitude']) :{'street_address': street_address}}}
		else:
			## if deviceID, tripnumber were already there then if event type was already present 
			if each['eventtype'] not in triptrack[each['deviceidentification']][each['tripnumber']].keys():
				## if eventtype is not there then adds it as new key and other keys
				triptrack[each['deviceidentification']][each['tripnumber']][each['eventtype']] = {(each['latitude']+","+each['longitude']) : {'street_address': street_address}}
			else:
				## if deviceID, tripnumber, eventtype were already there, then check the geocoords are aleady there
				if (each['latitude']+","+each['longitude']) not in triptrack[each['deviceidentification']][each['tripnumber']][each['eventtype']]:
					## if not there then add them as key and value is their corresponding street address.
					triptrack[each['deviceidentification']][each['tripnumber']][each['eventtype']][(each['latitude']+","+each['longitude'])] = {'street_address': street_address}

def distance_population(triptrack):
	""" this function takes a populated triptrack
			dictionary and inserts displacement in miles for every trip
			made by deviceID
			Args:	triptrack - a populated dictionary
	"""

	for eachid in triptrack.keys():
		## for every deviceID (first level key)
		for each in triptrack[eachid].keys():
			## for every tripnumber (second level key)
			## get the startevent first
			if 'TripStartEvent' in triptrack[eachid][each]:
				## get latitude and longitude of start event
				## which will be used as origin
				lat1 = list(triptrack[eachid][each]['TripStartEvent'].keys())[0].split(",")[0]
				long1 = list(triptrack[eachid][each]['TripStartEvent'].keys())[0].split(",")[1]
				## then get the latitude and longitude of 
				## other events of a trip which will be used as destination

				for eachtripevent in triptrack[eachid][each].keys():
					## for every tripevent (third level key)
					for eachgeocode in triptrack[eachid][each][eachtripevent].keys():
						## for every geo coordinates (fourth level key)
						lat2 = eachgeocode.split(",")[0]
						long2 = eachgeocode.split(",")[1]
						## gets the displacement between two geo coordinates
						## displacement is straight line distance on earth calculated by
						## using formula named haversine_distance
						straitdist = (haversine_distance((float(lat1), float(long1)), (float(lat2), float(long2)))*0.621371)
						triptrack[eachid][each][eachtripevent][eachgeocode].update({'distance': straitdist})
			else:
				## if startevent is not present then there is no
				## mechanism to select any other event as origin
				for eachtripevent in triptrack[eachid][each].keys():
					for eachgeocode in triptrack[eachid][each][eachtripevent]:
						triptrack[eachid][each][eachtripevent][eachgeocode].update({'distance':None})

	return triptrack

def store_newvins(query_time, bq_client):
  
  q_vinnumbers = '''SELECT VIN FROM `noted-casing-129313.geocode_converted_address.vindata`'''
  new_vins = query_time['mdiobdvin'].dropna().drop_duplicates(keep='first').values
  cached_vinnumbers = bq_client.query(q_vinnumbers).to_dataframe().values
	
  ## check for any new vin number in the new data ##
  temp = []
  for each in new_vins:
    if len(each) >= 17:
      if each not in cached_vinnumbers:
        temp.append(each)
  #################################################
  
  if temp:
    logging.info("Found New VIN")
    ## reading table schema for vindata table
    f = open('tableschema_vin.json', 'r')
    tableschema_vin = json.loads(f.read())
    f.close()
    #################################################
    
    ## getting new vindata from API and storing them 
    ## in vindata table
    master_vindata = []
    for each in new_vins:
      url = 'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValues/'+each+'?format=json'
      r = requests.get(url);
      vindata = json.loads(r.text)
      ## the data is at key Results and 
      ## contains only 1 element in a list
      vindata = vindata['Results'][0]
      column_header = list(vindata.keys()) 
      column_value = list(vindata.values())
      for i in range(0,len(column_value)):
        if column_value[i] == '':
          column_value[i] = None

      master_vindata.append(column_value)
    
    df_vin = pd.DataFrame(master_vindata)
    df_vin.columns = column_header
    pandas_gbq.to_gbq(df_vin, destination_table='geocode_converted_address.vindata',
                        project_id='noted-casing-129313', if_exists='append', table_schema=tableschema_vin,
                        credentials=oauth2.service_account.Credentials.from_service_account_file('noted-casing-129313-71580930e027.json'))	
    logging.info("Successfully stored New VINs in the table vindata")
  else:
    logging.info("No new VIN found ...")

def getANDstore_address_dict(address_dict, use_flag, bq_client):
  ## Query string to get those address which are already 
  ## present in AddressDB table and their latitude, longitude coordinates
  ## are in new rows.
  q_address = '''Select Distinct x.Latitude, x.Longitude, x.Street_Address 
									From `noted-casing-129313.geocode_converted_address.AddressDB` x INNER JOIN
                  	(SELECT latitude, longitude FROM `noted-casing-129313.stitch_caarmo.obd_data_track`
				            	Where recordedat >= (Select MAX(recordedat) 
				 				                           FROM `noted-casing-129313.geocode_converted_address.test_obd`)
				            	AND longitude IS NOT NULL AND LATITUDE IS NOT NULL AND LENGTH(cast(deviceidentification as string)) = 10) y 
                  ON x.latitude = y.latitude AND x.longitude = y.longitude'''


  query_addr = bq_client.query(q_address).to_dataframe()
  
  if use_flag == 0:
    logging.info('%d addresses were cached already!', len(query_addr))
    ## making a dictionary named address_dict of Street address
    ## where keys is (latitude, longitude) and 
    ## value is the corresponding street address
    ## of those geo coordinates and thi also
    ## avoid repeated values

    address_dict = {}
    for each in query_addr.values:
      lat = each[0]
      longi = each[1]
      address = each[2]
      address_dict[str(lat+","+longi)] = address

    return address_dict
  elif use_flag == 1:
    ## deletes some addresses from address_dict
    ## which are already present in the AddressDB
    ## so that we will store only new address and no
    ## duplicates to save space

    for each in query_addr.values:
      lat = each[0]
      longi = each[1]
      address = each[2]
      del address_dict[str(lat+","+longi)]
	
    ## makes a dataframe from address_dict to send
    ## it to the bigquery
    master_address = []
    for key in address_dict.keys():
      lat = key.split(",")[0]
      longi = key.split(",")[1]
      master_address.append([lat, longi, address_dict[key]])
    
    df_addr = pd.DataFrame(master_address, columns=['Latitude', 'Longitude', 'Street_Address'])
    logging.info("%d many address new to write!", len(df_addr))
    ## stores new discovered addresses to AddressDB
    pandas_gbq.to_gbq(df_addr, destination_table='geocode_converted_address.AddressDB',
                      project_id='noted-casing-129313', if_exists='append', 
                      credentials=oauth2.service_account.Credentials.from_service_account_file('noted-casing-129313-71580930e027.json'))	
    
    logging.info("AddressDB Updated Successfully")

def join_obd_vindata(bq_client):
  ## Update VIN rows and data
	query_obd_vin = '''SELECT a.*, b.AirBagLocCurtain, b.AirBagLocFront,	b.AirBagLocKnee,	b.AirBagLocSide,
						b.ABS,	b.AxleConfiguration,	b.Axles,	b.BasePrice,	
						b.BatteryA,	b.BedLengthIN,	b.BodyCabType, b.BodyClass,	
						b.BrakeSystemDesc,b.BrakeSystemType,	b.CurbWeightLB,	b.DaytimeRunningLight,
						b.DisplacementCC, b.DisplacementCI, b.DisplacementL,	b.Doors,
						b.DriveType, b.EngineConfiguration,	b.EngineCylinders,	b.EngineHP,	
						b.EngineManufacturer,	b.EngineModel,	b.ErrorText,b.FuelInjectionType,
						b.FuelTypePrimary, b.GVWR,	b.LaneDepartureWarning,	b.LaneKeepSystem,	
						b.LowerBeamHeadlampLightSource, b.Make,	b.Manufacturer,	b.ManufacturerId,
						b.Model,	b.ModelYear,	b.ParkAssist, b.PedestrianAutomaticEmergencyBraking,
						b.PlantCompanyName,	b.PlantCountry,	b.PlantState,	b.Pretensioner,	
						b.SeatBeltsAll, b.Series,	b.TPMS,	b.TractionControl,
						b.TrailerBodyType,	b.TrailerLength,	b.TrailerType, b.TransmissionSpeeds,
						b.TransmissionStyle,	b.Trim2,	b.VIN,	b.VehicleType,	
						b.WheelBaseLong,	b.WheelBaseShort,	b.WheelBaseType, b.WheelSizeFront,
						b.WheelSizeRear,	b.Wheels,	b.Windows
		 FROM `noted-casing-129313.geocode_converted_address.test_obd` a 
		 			LEFT OUTER JOIN `noted-casing-129313.geocode_converted_address.vindata` b
					ON a.mdiobdvin = b.VIN '''
	job_config = bigquery.QueryJobConfig(destination='noted-casing-129313.geocode_converted_address.obd_data_vin', write_disposition='WRITE_TRUNCATE')
	query_job = bq_client.query(query_obd_vin, job_config=job_config)
	query_job.result()

	logging.info("obd_vin_data table has been successfully updated")

def execute_query(bq_client, gc_reverseAPI, table_schema):
	"""this functions executes different queries and update tables
		 accordingly.
	Args:
	    bq_client: Object representing a reference to a BigQuery Client
			gc_reverseAPI : Object representing a reference to a HERE reverse Geocode API
			table_scheme : table schema of our big query table named temporary_obdtrack
	"""
	
	## Query string to get current table size
	q_len = '''Select Count(*) From `noted-casing-129313.geocode_converted_address.test_obd` '''
	## makes a request through big query client and converts results to dataframe
	## gets the values stored in dataframe in integer
	## which is length of current table
	oldlen = bq_client.query(q_len).to_dataframe().values[0]


	## Query string to get new rows in the table only for danlaw devices
	q_time = ''' SELECT * FROM `noted-casing-129313.stitch_caarmo.obd_data_track` 
				 Where recordedat >= (Select MAX(recordedat) 
				 					 FROM `noted-casing-129313.geocode_converted_address.test_obd`)
				 AND longitude IS NOT NULL AND LATITUDE IS NOT NULL AND LENGTH(cast(deviceidentification as string)) = 10 
				 '''

	query_time = bq_client.query(q_time).to_dataframe()
	newrows = len(query_time)
	
	## logging.info inserts the given string into the big query logs
	logging.info('%d new rows has discovered!', newrows)
 
	## Checks for any new VIN in the new data and
	## stored them in the table
	store_newvins(query_time, bq_client)

	## Appending the New VIN dataframes to the test_obd table
	## Getting all those rows from vindata table of which VIN
	## occurs in new data in test_obd and append to it
	xyz = '''SELECT b.* 
					 FROM `noted-casing-129313.geocode_converted_address.test_obd` a LEFT OUTER JOIN 
					 			`noted-casing-129313.geocode_converted_address.vindata` b 
								 ON a.mdiobdvin = b.VIN'''
	#########################################################
	
	## see function get_address_dict for more details
	## address_dict is a dictionary containing all cached
	## street address of geo coordinates
	address_dict = {}
	address_dict = getANDstore_address_dict(address_dict, 0, bq_client)
		
	## A dictionary triptrack which has keys of the form
	## deviceid, tripnumber, eventtype, (latitude,longitude)
	## which makes easy to calculate displacement of each trip
	## for every deviceID
	triptrack = {}

	## query_time is a dataframe, and apply is a method which applies 
	## function to every row in dataframe. Here we are only populating  
	## dictionaty triptrack using addressforevent function.
	query_time.apply(lambda x : addressforevent(gc_reverseAPI ,x, triptrack, address_dict) , axis=1)
	
	## now we have populated triptrack, where all keys and street address
	## are stored in triptrack, now adding displacement radius
	## in it using distance_population function
	triptrack = distance_population(triptrack)

	## new column in query_time dataframe named 'Street_Address'
	## in which all street addresses of latitude and longitude
	## are stored
	query_time['Street_Address'] = query_time.apply(lambda each : triptrack[each['deviceidentification']][each['tripnumber']][each['eventtype']][each['latitude']+","+each['longitude'] ]['street_address'], axis=1)
	
	## Displacement of all trips for every deviceID
	query_time['Displacement'] = query_time.apply(lambda each : triptrack[each['deviceidentification']][each['tripnumber']][each['eventtype']][each['latitude']+","+each['longitude'] ]['distance'], axis=1)
	
	## Date conversion to YYYY-MM-DD from recordedat column
	query_time['recordedatdate'] = query_time.apply(lambda each: pd.to_datetime(each['recordedat'],format='%Y-%m-%d').strftime(format='%Y-%m-%d'), axis=1)	#print("writing to bq")

	## Store new street address into AddressDB
	## see function for more details 
	getANDstore_address_dict(address_dict,1, bq_client)

	## this function takes dataframe query_time and append it to the table
	## test_obd using our google cloud credientials and table_schema
	pandas_gbq.to_gbq(query_time, destination_table='geocode_converted_address.test_obd',
	                      project_id='noted-casing-129313', if_exists='append', table_schema = table_schema, 
	                      credentials=oauth2.service_account.Credentials.from_service_account_file('noted-casing-129313-71580930e027.json'))
	logging.info("test_obd table has been updated successfully")
	
	## Query to get new length of the table
	q_len = '''Select Count(*) From `noted-casing-129313.geocode_converted_address.test_obd` '''
	newlen = bq_client.query(q_len).to_dataframe().values[0]
	
	## A check to check if all rows have been successfully
	## inserted into the table, therefore, old table length and
	## newrows sum must be equal to newlength of the table
	if (newrows + oldlen) == newlen:
		## if true then delete the old temporary_obdtrack
		bq_client.delete_table('noted-casing-129313.geocode_converted_address.temporary_obdtrack', not_found_ok=True)
		logging.info('Table deleted successfully')

		## And copy the updated test_obd table to temporary_obdtrack
		job = bq_client.copy_table('noted-casing-129313.geocode_converted_address.test_obd', 
                        'noted-casing-129313.geocode_converted_address.temporary_obdtrack')
		job.result()  # Wait for the job to complete.
		logging.info('Table copied successfully')

		## Now update the obd_vin_date table
		join_obd_vindata(bq_client)

	else:
		## if lengths are not equal, then log an error
		## in bigquery logs.
		logging.error('Table not updated because length mismatch!')	
	
	logging.info('Function Completed!')

def main(date, context):
	""" Caller Function which calls and execute queries from
			other functions"""
	
	## API key obtained from HERE Maps account
	
	#HERE_API_Key = 'yaOI3bLuFytv7ad3TsDkG8Of5jAGm-v1yvtoinisSzw'
	HERE_API_Key = '556vCDXlklLOuswiO_mjslzAsjdD5IrM7aJnXozq560'
	## object of bigquery API regiestered with our google cloud
	## account credientials in file 'noted-casing-129313-71580930e027.json'
	## through this client we will make reuquest of query to big query table

	bq_client = bigquery.Client.from_service_account_json('noted-casing-129313-71580930e027.json')
	
	## this object is for HERE API requests
	gc_reverseAPI = herepy.GeocoderReverseApi(HERE_API_Key)

	try:
		current_time = datetime.datetime.utcnow()
		log_message = Template('Cloud Function was triggered on $time')
		logging.info(log_message.safe_substitute(time=current_time))

		try:
			## table schema of our big query table
			## stored in json, this is used to tell
			## by a function (appending data to table) to 
			## correctly append every column

			f = open('tableschema.json')
			table_schema = json.loads(f.read())
			f.close()

			execute_query(bq_client, gc_reverseAPI, table_schema)

		except Exception as error:
			log_message = Template('Query failed due to '
									'$message.')
			logging.error(log_message.safe_substitute(message=error))

	except Exception as error:
		log_message = Template('$error').substitute(error=error)
		logging.error(log_message)

if __name__ == '__main__':
	main('date', 'context')
