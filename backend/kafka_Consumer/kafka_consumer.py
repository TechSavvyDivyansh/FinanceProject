# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, to_timestamp, expr, split
# from pyspark.sql.types import DoubleType
# import time

# # Kafka & MySQL Config
# KAFKA_BROKER = "localhost:9092"
# TOPIC_NAME = "transaction-stream"
# MYSQL_JDBC_DRIVE = "com.mysql.cj.jdbc.Driverr"
# MYSQL_URL = "jdbc:mysql://localhost:3306/finance_megaProject_db"
# DB_TABLE = "transactions"
# MYSQL_USER = "root"
# MYSQL_PASSWORD = "111"

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("MySQL JDBC Example") \
#     .config("spark.jars", "/usr/share/java/mysql-connector-java-9.2.0.jar") \
#     .getOrCreate()

# # Read from Kafka
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", KAFKA_BROKER) \
#     .option("subscribe", TOPIC_NAME) \
#     .option("startingOffsets", "latest") \
#     .load()

# # Deserialize Kafka Messages (CSV Format)
# df = df.selectExpr("CAST(value AS STRING)")

# # Split CSV String into Columns
# df = df.withColumn("date", split(col("value"), ",")[0]) \
#        .withColumn("transaction_narration", split(col("value"), ",")[1]) \
#        .withColumn("transaction_id", split(col("value"), ",")[2]) \
#        .withColumn("debit", split(col("value"), ",")[3]) \
#        .withColumn("credit", split(col("value"), ",")[4]) \
#        .withColumn("balance", split(col("value"), ",")[5])

# # Convert Data Types
# df = df.withColumn("date", to_timestamp(col("date"), "dd/MM/yyyy")) \
#        .withColumn("debit", col("debit").cast(DoubleType())) \
#        .withColumn("credit", col("credit").cast(DoubleType())) \
#        .withColumn("balance", col("balance").cast(DoubleType()))

# # Remove Duplicates Using Watermarking
# df = df.withWatermark("date", "1 minute").dropDuplicates(["transaction_id"])

# # Fill Missing Values
# df = df.na.fill({"debit": 0.0, "credit": 0.0, "balance": 0.0})

# # Categorize Transactions
# df = df.withColumn("category",
#                    expr("CASE WHEN transaction_narration LIKE '%Refund%' THEN 'Refund' " +
#                         "WHEN transaction_narration LIKE '%Salary%' THEN 'Salary' " +
#                         "WHEN transaction_narration LIKE '%FD%' THEN 'Fixed Deposit' " +
#                         "WHEN transaction_narration LIKE '%Withdrawal%' THEN 'Withdrawal' " +
#                         "ELSE 'Others' END"))

# # Write Streaming Data to MySQL Using ForeachBatch
# def write_to_mysql(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("driver", MYSQL_JDBC_DRIVE) \
#         .option("url", MYSQL_URL) \
#         .option("dbtable", DB_TABLE) \
#         .option("user", MYSQL_USER) \
#         .option("password", MYSQL_PASSWORD) \
#         .append()\
#         .save()

# query = df.writeStream \
#     .foreachBatch(write_to_mysql) \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/spark-checkpoints") \
#     .start()

# query.awaitTermination()



from kafka import KafkaConsumer, KafkaProducer
import mysql.connector
import json
from datetime import datetime

# Kafka Configuration
KAFKA_BROKER = "localhost:9092"
SOURCE_TOPIC = "transaction-stream"
DEST_TOPIC = "processed-transactions"
GROUP_ID = "transaction-consumer-group"

# MySQL Configuration
MYSQL_HOST = "localhost"
MYSQL_DATABASE = "finance_megaProject_db"
MYSQL_USER = "root"
MYSQL_PASSWORD = "111"

# Kafka Consumer Setup
consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    group_id=GROUP_ID,
    auto_offset_reset="latest",
    value_deserializer=lambda x: x.decode("utf-8")  # Convert bytes to string
)

# Kafka Producer Setup
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")  # Convert dict to JSON string
)

# MySQL Connection
def get_mysql_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )

def convert_date_format(date_str):
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
    except ValueError:
        print(f"Error: Incorrect date format {date_str}")
        return None
    


# Function to Insert Data into MySQL
def insert_into_mysql(data):
    try:
        conn = get_mysql_connection()
        cursor = conn.cursor()

        sql_query = """
        INSERT INTO transactions (date, transaction_narration, transaction_id, debit, credit, balance, category)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        cursor.execute(sql_query, data)
        conn.commit()

        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")

def consume_messages():
    for msg in consumer:
        try:
            value = json.loads(msg.value)  # Deserialize JSON message

            date = convert_date_format(value["date"])  # Expected format: dd/MM/yyyy
            transaction_narration = value["transaction_narration"]
            transaction_id = value["transaction_id"]
            debit = float(value["debit"]) if value["debit"] else 0.0
            credit = float(value["credit"]) if value["credit"] else 0.0
            balance = float(value["balance"]) if value["balance"] else 0.0

            transaction_narration=transaction_narration.upper()

            # Categorize transactions
            if "REFUND" in transaction_narration:
                category = "Refund"
            elif "SALARY" in transaction_narration:
                category = "Salary"
            elif "FD" in transaction_narration:
                category = "Fixed Deposit"
            elif "UPI" in transaction_narration:
                category = "UPI"
            elif "NEFT" in transaction_narration:
                category = "NEFT"
            elif "FD" in transaction_narration:
                category = "Fixed Deposit Booked"
            elif "IMPS" in transaction_narration:
                category = "IMPS transfer"
            elif "ATM WITHDRAWAL" in transaction_narration:
                category = "ATM Withdrawal"
            elif "CHEQUE DEPOSIT" in transaction_narration:
                category = "Cheque Deposit"
            elif "CASH DEPOSIT" in transaction_narration:
                category = "Cash Deposit"
            else:
                category = "Others"

            # Data to insert into MySQL
            mysql_data = (date, transaction_narration, transaction_id, debit, credit, balance, category)
            insert_into_mysql(mysql_data)

            # Data to send to new Kafka topic
            processed_data = {
                "date": date,
                "transaction_narration": transaction_narration,
                "transaction_id": transaction_id,
                "debit": debit,
                "credit": credit,
                "balance": balance,
                "category": category
            }

            # Send processed data to new Kafka topic
            producer.send(DEST_TOPIC, processed_data)
            print(f"Processed & Sent to {DEST_TOPIC}: {processed_data}")

        except json.JSONDecodeError:
            print(f"Error: Received non-JSON message: {msg.value}")
        except KeyError as e:
            print(f"Error: Missing key {e} in message: {msg.value}")
        except ValueError as e:
            print(f"Error: Data conversion issue: {e} in message: {msg.value}")
        except Exception as e:
            print(f"Error processing message: {e}")


# Run Consumer
if __name__ == "__main__":
    try:
        consume_messages()
    except KeyboardInterrupt:
        print("Stopping consumer...")
    finally:
        consumer.close()
        producer.close()
