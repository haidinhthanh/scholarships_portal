import mysql.connector
import pandas as pd
import numpy as np


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="haidt261qaz@",
  database="scholarship_management"
)

print('db conncect')
mycursor = mydb.cursor()

df = pd.read_csv('data_2.csv', sep=',')
df = df.replace(np.nan, None)
sql = "INSERT INTO scholarships (title, deadline, provider, provider_type, amount, location, description, eligibility, fields_of_study, target_country, coverage, link, number_of_scholarships) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
val = [
    (row['title'], row['deadline'], row['provider'], row['provider_type'], row['amount'], row['location'], row['description'], row['eligibility'], row['fields_of_study'], row['target_country'], row['coverage'], row['link'], row['number_of_scholarships'])
    for idx, row in df.iterrows()
]
print("create query")
mycursor.executemany(sql, val)

print('exe')
mydb.commit()

print(mycursor.rowcount, "was inserted.")
# {
#     "title": "",
#     "deadline": ""
#     "provider": "",
#     "provider_type": "",
#     "amount": "",
#     "location": "",
#     "description": "",
#     "eligibility": "",
#     "fields_of_study": "",
#     "target_country": "",
#     "coverage": "",
#     "link": ""
# }