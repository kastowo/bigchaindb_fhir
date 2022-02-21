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

            #asset definition
            patient = {
                'data': data_json
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
                asset=patient,
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