import csv
import pymysql.cursors
import time
import uuid

# Connect to the database
connection = pymysql.connect(host='localhost',
                             user='root',
                             password='root',
                             database='openemr',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

# mydb = MySQLdb.connect(host='localhost',
#     user='root',
#     passwd='root',
#     db='openemr')

cursor = connection.cursor()

start_time = time.time()
with open("./csv/patient_3000.csv", "r") as f:
    reader = csv.DictReader(f, delimiter=",")
    pid = 1
    for row in reader:
        firstname = row['firstname']
        lastname = row['lastname']
        lastname = row['lastname']
        birthdate = row['birthdate']
        birth_maturity = row['birth_maturity']
        gender = row['gender']
        
        if gender == "M":
            gender = "male"
        
        if gender == "F":
            gender = "female"

        address = row['address']
        city = row['city']
        state = row['state_id']
        country = row['country_id']
        postcode = row['postcode']
        phone = row['phone']
        email = row['email']
        medical_record_id = row['medical_record_id']

        # Create a new record
        sql = "INSERT INTO `patient_data` (`pid`, `fname`, `lname`, `DOB`, `street`, `postal_code`, `city`, `state`, `country_code`, `phone_cell`, `email`, `sex`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, (pid, firstname, lastname, birthdate, address, postcode, city, state, country, phone, email, gender))
        pid += 1
#close the connection to the database.
connection.commit()
cursor.close()
print("Done")
print("--- %s seconds ---" % (time.time() - start_time))