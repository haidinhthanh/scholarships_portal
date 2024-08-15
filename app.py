from flask import Flask, render_template, request
import mysql
from pyspark.sql import SparkSession


app = Flask(__name__)

def get_db_connection():
    return mysql.connector.connect(
        host="root",  # Replace with your MySQL server address
        user="your_username",  # Replace with your MySQL username
        password="your_password",  # Replace with your MySQL password
        database="your_database"  # Replace with your MySQL database name
    )

def get_spark_session():
    return SparkSession.builder \
        .appName("Flask_PySpark_MySQL") \
        .config("spark.jars", "/path/to/mysql-connector-java-8.0.32.jar") \
        .getOrCreate()


@app.route("/home", methods=['GET', 'POST'])
def home():
    method = request.method
    if method == "GET":
        return render_template("index.html", data=[])
    else:
        country = request.form.get('country')
        deadline = request.form.get('deadline')
        major = request.form.get('major')

        spark = get_spark_session()
        # MySQL connection properties
        jdbc_url = "jdbc:mysql://localhost:3306/your_database"
        connection_properties = {
            "user": "your_username",
            "password": "your_password",
            "driver": "com.mysql.cj.jdbc.Driver"
        }

        # Load data from MySQL table into a DataFrame
        df = spark.read.jdbc(url=jdbc_url, table="your_table", properties=connection_properties)

        # Perform some query using PySpark SQL (optional)
        table_view = "your_table_view"
        df.createOrReplaceTempView(table_view)
        

        query = f"SELECT id, name, email FROM {table_view}"
        if country or deadline or major:
            conditions = []
            if country:
                conditions.append(f" location like '%{country}%' ")
            if deadline:
                conditions.append(f" deadline < '{deadline}' ")
            if major:
                conditions.append(f" fields_of_study like '%{major}%' ")

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
        return render_template("index.html", data=data)


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