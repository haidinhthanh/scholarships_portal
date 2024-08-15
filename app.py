from flask import Flask, render_template, request
import mysql
from pyspark.sql import SparkSession


app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="haidt261qaz@",
        database="scholarship_management"
    )

def get_spark_session():
    return SparkSession.builder \
        .appName("Flask_PySpark_MySQL") \
        .config("spark.jars", "mysql-connector-j-9.0.0/mysql-connector-j-9.0.0.jar") \
        .getOrCreate()


@app.route("/home", methods=['GET', 'POST'])
def home():
    method = request.method
    if method == "GET":
        return render_template("index.html", data=[], country='', deadline='', major='')
    else:
        country = request.form.get('country')
        deadline = request.form.get('deadline')
        major = request.form.get('major')

        # db = get_db_connection()
        # cursor = db.cursor()

        # table_view = ''
        # # Query the database for all users
        # query = f"SELECT * FROM {table_view}"
        # if country or deadline or major:
        #     conditions = []
        #     if country:
        #         conditions.append(f" location like '%{country}%' ")
        #     if deadline:
        #         conditions.append(f" deadline < '{deadline}' ")
        #     if major:
        #         conditions.append(f" fields_of_study like '%{major}%' ")

        #     if len(conditions) == 1:
        #         query += " WHERE " + conditions[0]
            
        #     else:
        #         query +=  " WHERE " + "AND".join(conditions)

        # else:
        #     query += " LIMIT 20"

        # cursor.execute(query)
        # data = cursor.fetchall()

        # # Close the database connection
        # cursor.close()
        # db.close()

        # return data


        spark = get_spark_session()
        # MySQL connection properties
        jdbc_url = "jdbc:mysql://localhost:3306/scholarship_management"
        connection_properties = {
            "user": "root",
            "password": "haidt261qaz@",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Load data from MySQL table into a DataFrame
        df = spark.read.jdbc(url=jdbc_url, table="scholarships", properties=connection_properties)

        table_view = "test"
        df.createOrReplaceTempView(table_view)
        

        query = f"SELECT * FROM {table_view}"
        if country or deadline or major:
            conditions = []
            if country:
                conditions.append(f" location like '%{country}%' ")
            if deadline:
                conditions.append(f" deadline < '{deadline}' ")
            if major:
                conditions.append(f" (fields_of_study like '%{major}%' or fields_of_study = 'Unrestricted')")

            if len(conditions) == 1:
                query += " WHERE " + conditions[0]
            
            else:
                query +=  " WHERE " + "AND".join(conditions)

        else:
            query += " LIMIT 20"

        result_df = spark.sql(query)

        # Collect the data into a list of rows
        data = result_df.collect()
        data = [row.asDict() for row in data]
        return render_template("index.html", data=data, major=major, country=country, deadline=deadline)


if __name__ == "__main__":
    app.run()


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