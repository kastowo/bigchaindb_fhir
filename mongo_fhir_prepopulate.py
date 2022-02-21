import csv
import pymongo
import time
from bson.objectid import ObjectId

# Connect to the database
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["tesis"]
mycol = mydb["patient_fhir_data"]

start_time = time.time()
patient = {
                "resourceType": "Patient",
                "identifier": [
                    {
                        "use": "usual",
                        "type": {
                            "coding": [
                            {
                                "system": "http://hl7.org/fhir/ValueSet/identifier-type",
                                "code": "MR",
                                "display": "Medical record number"
                            }
                            ],
                            "text": ""
                        },
                        "system": "",
                        "value": "",
                        "period": {},
                        "assigner": {}
                    }
                ],
                "active": "true",
                "name": [
                    {
                        "use": "usual",
                        "text": "",
                        "family": "",
                        "given": [],
                        "prefix": [],
                        "suffix": [],
                        "period": {}
                    }
                ],
                "telecom": [
                    {
                        "system": "phone",
                        "value": "",
                        "use": "mobile",
                        "rank": "1",
                        "period": {}
                    },
                    {
                        "system": "email",
                        "value": "",
                        "use": "mobile",
                        "rank": "2",
                        "period": {}
                    }
                ],
                "gender": "",
                "birthDate": "",
                "deceasedBoolean": "",
                "deceasedDateTime": "",
                "address": [
                    {
                        "use": "home",
                        "type": "physical",
                        "text": "",
                        "line": [],
                        "city": "",
                        "district": "",
                        "state": "",
                        "postalCode": "",
                        "country": "",
                        "period": {}
                    }
                ],
                "maritalStatus": {
                    "coding": [
                        {
                            "system": "http://hl7.org/fhir/ValueSet/marital-status",
                            "code": "",
                            "display": ""
                        }
                    ],
                    "text": "A current marriage contract is active"
                },
                "multipleBirthBoolean": "",
                "multipleBirthInteger": "",
                "photo": [
                    {
                        "contentType": "",
                        "\\language\\": "",
                        "data": "",
                        "url": "",
                        "size": "",
                        "hash": "",
                        "title": "",
                        "creation": ""
                    }
                ],
                "contact": [
                    {
                        "relationship": [
                            {
                            "coding": [
                                {
                                    "system": "http://hl7.org/fhir/ValueSet/patient-contactrelationship",
                                    "code": "C",
                                    "display": "Emergency Contact"
                                }
                            ],
                            "text": ""
                            }
                        ],
                        "name": {
                            "use": "",
                            "text": "",
                            "family": "",
                            "given": [],
                            "prefix": [],
                            "suffix": [],
                            "period": {}
                        },
                        "telecom": [
                            {
                            "system": "http://hl7.org/fhir/ValueSet/contact-point-system",
                            "value": "",
                            "use": "phone",
                            "rank": "1",
                            "period": {}
                            }
                        ],
                        "address": {
                            "use": "home",
                            "type": "physical",
                            "text": "",
                            "line": [],
                            "city": "",
                            "district": "",
                            "state": "",
                            "postalCode": "",
                            "country": "",
                            "period": {}
                        },
                        "gender": "",
                        "organization": {},
                        "period": {}
                    }
                ],
                "communication": [
                    {
                        "\\language\\": {
                            "coding": [
                            {
                                "system": "http://hl7.org/fhir/ValueSet/languages",
                                "code": "id",
                                "display": "Indonesia"
                            }
                            ],
                            "text": ""
                        },
                        "preferred": "true"
                    }
                ],
                "generalPractitioner": [
                    {}
                ],
                "managingOrganization": {},
                "link": [
                    {
                        "other": {},
                        "type": ""
                    }
                ]
            }

with open("./csv/patient_3000.csv", "r") as f:
    reader = csv.DictReader(f, delimiter=",")
    pid = 1
    
    for row in reader:
        patient["identifier"][0]["value"] = row['medical_record_id']
        patient["name"][0]["text"] = "{}." ".{}".format(row['firstname'], row['lastname']) 
        patient["birthDate"] = row['birthdate'] 
         
        # data_json['birth_maturity'] = row['birth_maturity']
        gender = row['gender']
        
        if gender == "M":
            patient['gender'] = "male"
        
        if gender == "F":
            patient['gender'] = "female"

        patient['address'][0]['text'] = row['address']
        patient['address'][0]['city'] = row['city']
        patient['address'][0]['state'] = row['state_id']
        
        patient['address'][0]['country'] = row['country_id']
        patient['address'][0]['postalCode'] = row['postcode']
        patient['telecom'][0]['value'] = row['phone']
        patient['telecom'][1]['value'] = row['email']
        
        print(patient)
        break
        # Create a new record
        # list_data.append(data_json)
        if '_id' in patient:
            del patient['_id']
        
        x = mycol.insert_one(patient)

#close the connection to the database.
myclient.close()
print("Done")
print("--- %s seconds ---" % (time.time() - start_time))