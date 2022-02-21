import csv
import pymongo
import time
from bson.objectid import ObjectId

# Connect to the database
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["tesis"]
mycol = mydb["patient_data"]

start_time = time.time()
list_data = []
with open("./csv/patient_3000.csv", "r") as f:
    reader = csv.DictReader(f, delimiter=",")
    pid = 1
    data_json = {}
    for row in reader:
        # data_json['_id'] = ObjectId() 
        data_json['firstname'] = row['firstname']
        data_json['lastname'] = row['lastname']
        data_json['lastname'] = row['lastname']
        data_json['birthdate'] = row['birthdate']
        data_json['birth_maturity'] = row['birth_maturity']
        gender = row['gender']
        
        if gender == "M":
            data_json['gender'] = "male"
        
        if gender == "F":
            data_json['gender'] = "female"

        data_json['address'] = row['address']
        data_json['city'] = row['city']
        data_json['state'] = row['state_id']
        data_json['country'] = row['country_id']
        data_json['postcode'] = row['postcode']
        data_json['phone'] = row['phone']
        data_json['email'] = row['email']
        # data_json['medical_record_id'] = row['medical_record_id']

        # Create a new record
        # list_data.append(data_json)
        if '_id' in data_json:
            del data_json['_id']
        
        x = mycol.insert_one(data_json)

#close the connection to the database.
myclient.close()
print("Done")
print("--- %s seconds ---" % (time.time() - start_time))