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

# Select / retrieve
sql = "SELECT * FROM patient_data LIMIT 3000"
cursor.execute(sql)
result = cursor.fetchall()
print(result)
#close the connection to the database.
cursor.close()
print("Done")
print("--- %s seconds ---" % (time.time() - start_time))