
#Load csv to MongoDB
import pymongo
from pymongo import MongoClient

#Connection
con = MongoClient('localhost',27017)
db = con.measures_hourly_test #Database
 
lines= open('measures_hourly_test.csv').readlines()


for line in lines :
	if  (line.split(';')[2] !='NULL'): #Measure not null
 		db.measures.insert_one({ 'ref':line.split(';')[0],
	                             'date':line.split(';')[1],
	                             'measure': int(line.split(';')[2]),
	                             'period': line.split(';')[3],
                                 'unit': line.split(';')[4],
                                 'type': line.split(';')[5]
                            })



