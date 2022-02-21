import csv
import pymongo
import time
from bson.objectid import ObjectId

# Connect to the database
myclient = pymongo.MongoClient("mongodb://192.168.100.22:27017/")
mydb = myclient["bigchain"]
mycol = mydb["asset"]

start_time = time.time()
result = mycol.find().limit(3000)
print(result)

#close the connection to the database.
myclient.close()
print("Done")
print("--- %s seconds ---" % (time.time() - start_time))
