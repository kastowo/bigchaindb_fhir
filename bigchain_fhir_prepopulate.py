import json
import time
import csv

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.exceptions import NotFoundError
from bigchaindb_driver.crypto import generate_keypair


if __name__ == "__main__":
    # print('Loading json files...')
    # with open('tx_create.json', 'r') as f:
    #     tx_create = json.load(f)
    # with open('tx_transfer.json', 'r') as f:
    #     tx_transfer = json.load(f)

    bdb_root_url = 'http://localhost:9984'
    bdb = BigchainDB(bdb_root_url)
  
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
    
    with open("./csv/patient_200.csv", "r") as f:
        reader = csv.DictReader(f, delimiter=",")
        
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

            #asset definition
            patient_asset = {
                'data': patient
            }

            #metadata definition
            # metadata = {}

            #set user owner (test)
            alice = generate_keypair()
            
            # Asset Creation
            # First, let’s prepare the transaction:
            prepared_creation_tx = bdb.transactions.prepare(
                operation='CREATE',
                signers=alice.public_key,
                asset=patient_asset,
                # metadata=metadata,
            )

            # print(prepared_creation_tx)

            # The transaction now needs to be fulfilled by signing it with Alice’s private key:
            fulfilled_creation_tx = bdb.transactions.fulfill(
                prepared_creation_tx, private_keys=alice.private_key
            )

            # And sent over to a BigchainDB node:
            sent_creation_tx = bdb.transactions.send_commit(fulfilled_creation_tx)

            # txid = fulfilled_creation_tx['id']

            # print(txid)

        print('Done!')
        print("--- %s seconds ---" % (time.time() - start_time))
