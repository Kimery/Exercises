from copy import deepcopy
import sys
import os
import requests
import json
import csv
import datetime
import math 
import pytz
from multiprocessing import Process, Pool
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from requests.packages.urllib3 import disable_warnings

disable_warnings(InsecureRequestWarning)


##URLs
base_url_measures = 'https://api.empowering.cimne.com/v1/amon_measures'
base_url_log = 'https://api.empowering.cimne.com/authn/login'
base_url_measures_measurements= 'https://api.empowering.cimne.com/v1/amon_measures_measurements'

headers = {
    'X-CompanyId': '1234509876',
    'content-type': 'application/json'
}

# When login CompanyId must be excluded from headers
headers_login = {
    'content-type': 'application/json'
}
# User and Password
payload = {
    "username": "test@test",
    "password":"test1234"
}

##Certificates
cert_file_path= 'C:/Users/Carolina/Desktop/BeeData/Desa/Cert/client0-api-cimne.crt'
key_file_path = 'C:/Users/Carolina/Desktop/BeeData/Desa/Cert/client0-api-cimne.key'
cert = (cert_file_path, key_file_path)

# Obtaining the token from the login, this information is needed to
# perform the request to the new server
def login():
    res = requests.post(base_url_log, headers=headers_login, data=json.dumps(payload), verify=False)
    cookie = res.json()['token']
    cookie = {
            'iPlanetDirectoryPro': cookie
    }
    return cookie
         


cookie = login()

import csv
import time

with open('data.csv','r') as f:
    lineas= f.read().splitlines() #List of lines
    
 	  #Value of the column unit measure
    measure_unit= lineas[0].split(";")[1]
     
    if (measure_unit.strip()=='kWh') :
      reading_detail = {"type": "electricityConsumption", "period": "INSTANT", "unit": "kWh"}      
      type_m = "electricityConsumption"
    else:
      reading_detail = {"type": "electricityKiloVoltAmpHours", "period": "INSTANT", "unit": "kVArh"}
      type_m= "electricityKiloVoltAmpHours"
  
    lineas.pop(0) #Delete row with columns name
    
    #Document structure to process

    amon_measures_in={ "measurements":[],
                       "meteringPointId": "c1810810-0381-012d-25a8-0017f2cd8888",  
                       "readings": [reading_detail],
                       "deviceId": "c1810810-0381-012d-25a8-0017f2cd8888"
                     }
    c=0
    r=0
  
  #Read each row of the file
    for l in lineas:
        
        linea= l.split(';')        
        r=r+1
        
        if (linea[0]!=''and linea[1]!=''): #Datetime and measure must be inform

            fecha=linea[0].strip()
            v_value = float(linea[1])

            v_timestamp=datetime.datetime(int(fecha[0:4]),int(fecha[5:7]),int(fecha[8:10]),int(fecha[11:13]),int(fecha[14:16]),int(fecha[17:19]) )#linea[0].strip() 
            v_isotimestamp=v_timestamp.isoformat()+'Z' #2014-10-11T16:37:05Z
               
    
        #Measure document structure 
            measure =  {
                          "timestamp":v_isotimestamp,  
                          "type": type_m, 
                          "value": v_value  
                        }        
        
		        #Add measure on measurements[]    
            amon_measures_in['measurements'].append(measure)                    
            c=c+1


v_size_tot=  len(json.dumps(amon_measures_in))
print('Total size of the document:'+ str(v_size_tot))
print('Read rows:'+str(r))
print('Load rows:'+str(c))
    
start = time.strftime('%c')
res = requests.request('POST', base_url_measures, headers=headers, cookies=cookie, data=json.dumps(amon_measures_in), cert=cert, verify=False)    
end = time.strftime('%c')
		
print(str(res))
print('Start:'+ start + ' End:'+end)
    
    
#*****GET
#res_get = requests.request('GET', base_url_measures_measurements, headers=headers, cookies=cookie,params= '',cert=cert, verify=False)
#print(res_get.json())
#print('Total results: '+ str(res_get.json()['_meta']['total']))
    


