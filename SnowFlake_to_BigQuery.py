import snowflake.connector as sf
import pandas as pd
import pandas_gbq
from google import oauth2
import json

## This scripts extracts data from Snowfalke of client's data warehouse and sends that data to BigQuery data warehouse.

sfAccount = '***'
sfUser = '***'
sfPswd = '***'

sfConnection = sf.connect(
      user=sfUser,
      password=sfPswd,
      account=sfAccount
  )

print("OK!")
sfq = sfConnection.cursor()
sfq.execute('use warehouse CAARMO')
sfq.execute(''' Select * From "ANALYTICS"."SCH_TASKS"."CAARMO_APPOINTMENT_DATA" Where date > '2021-08-23' ''')
sfResults = sfq.fetchall()
cols = ["APPOINTMENT_ID","SERVICE_TYPE","DATE","TIME_IN_UTC","TIME_OUT_UTC","TIME_IN_LOCAL","TIME_OUT_LOCAL","EMPLOYEE_ID","EMPLOYEE_NAME","EMPLOYEE_EMAIL","LAT","LNG","AMOUNT_BILLED","AMOUNT_COLLECTED", "COMPLETED", "SCHEDULED"]

f = open('Appointments_tableschema.json')
table_schema = json.loads(f.read())
f.close()

db = pd.DataFrame(sfResults, columns=cols)
db["EMPLOYEE_ID"] = db["EMPLOYEE_ID"].astype(float)

pandas_gbq.to_gbq(db, destination_table='Aptive.Appointments',
                                project_id='noted-casing-129313', if_exists='append',# table_schema=table_schema,
                            credentials=oauth2.service_account.Credentials.from_service_account_file('notedcasingbqcreds.json'))

sfq.close()
sfConnection.close()