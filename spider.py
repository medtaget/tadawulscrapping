import requests
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from gspread_formatting import *
import schedule
import time
from datetime import datetime 
import pytz 



 

 
def job():
 scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]


 UTC = pytz.utc 
 timeZ_Kl = pytz.timezone('Asia/Riyadh') 
 dt_Kl = datetime.now(timeZ_Kl) 
 print("I'm working...")
 url='https://www.tadawul.com.sa/wps/portal/tadawul/markets/equities?locale=ar'

 headers={
 'user-agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:84.0) Gecko/20100101 Firefox/84.0'
 }
 r=requests.get(url,headers=headers)
 soup=BeautifulSoup(r.content,'html.parser')
 print('Start extracting...')
 rows=soup.select('#layoutContainers > div > div.row > div.col-xs-14.col-md-11.col-lg-9 > div:nth-child(1) > div.component-container.wpthemePrimaryContainer.wpthemeCol.ibmDndColumn.col-sm-7.col-xs-14.id-Z7_NHLCH082KGRH80A64N55590GU6 > div > section > div.table_wrap')[0].find('table',{'id':'gainersTable'}).find('tbody').findAll('tr')
 dname=['Date','Time','Company']
 dchange=[dt_Kl.strftime('%d-%m'),dt_Kl.strftime('%H:%M'),'%']
 for row in rows:
  name=row.findAll('td')[0].text.strip()
  print(name)
  change=row.findAll('td')[3].text
  print(change)
  dname.append(name)
  dchange.append(change)
 print('Writing CSV file')
 df = pd.read_csv("data.csv")
 print(dname)
 print(dchange)
 print(len(dname))
 print(len(dchange))
 df.insert(loc=len(df.columns), column=' ' , value=dname, allow_duplicates=True)
 df.insert(loc=len(df.columns), column=' ', value=dchange, allow_duplicates=True)
 df.to_csv('data.csv', index=False)
 
 print('Saved CSV file.') 
 credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)
 client = gspread.authorize(credentials)

 spreadsheet = client.open('STOCKS_SCRAPER')


 with open('data.csv', 'r' , encoding='latin-1') as file_obj:
    content = file_obj.read()
    client.import_csv(spreadsheet.id, data=content)
 worksheet = spreadsheet.worksheet('STOCKS_SCRAPER')
 worksheet.format("1", {
 "backgroundColor": {
   "red": 0.92,
   "green": 0.92,
   "blue": 0.92
     },
  "horizontalAlignment": "CENTER",
  "textFormat": {
      "foregroundColor": {
        "red": 0.92,
        "green": 0.92,
        "blue": 0.92
      },
      "fontSize": 0,
   
    }
 })
 worksheet.format("2:10", {
 "backgroundColor": {
   "red": 0.92,
   "green": 0.92,
   "blue": 0.92
    },
  "horizontalAlignment": "CENTER",
  "textFormat": {
      "fontSize": 12,
      "bold": True
    }

  })

 set_column_width(worksheet, 'A', 5)
 set_column_width(worksheet, '1', 5)
 set_column_width(worksheet, '10', 5)
   


for i in range(1,10):
 job()


