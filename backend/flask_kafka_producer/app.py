from flask import Flask,request,jsonify
from kafka import KafkaProducer
import csv
import io

app=Flask(__name__)

KAFKA_BROKER="localhost:9092"
TOPIC_NAME="transaction-stream"


# KAFKA producer
producer=KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v:v.encode("utf-8")
)


@app.route("/upload",methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    file = request.files['file']
    content = file.stream.read().decode("utf-8")  # Read CSV content
    reader = csv.reader(io.StringIO(content))

    for index,row in enumerate(reader):
        if(index==0):
            continue
        print("row",row)
        transaction_data = ",".join(row)  # Convert CSV row to string
        # print("transaction data",transaction_data,"\n")
        producer.send(TOPIC_NAME, key=bytes(row[0], 'utf-8'), value=transaction_data)

    producer.flush()  # Ensure messages are sent
    return jsonify({"message": "Transactions sent to Kafka","data":transaction_data}), 200




if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)

