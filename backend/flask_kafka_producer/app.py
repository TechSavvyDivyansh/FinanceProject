from flask import Flask, request, jsonify
from kafka import KafkaProducer
import csv
import io
import json

app = Flask(__name__)

KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "transaction-stream"

# Kafka Producer
producer=KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v:json.dumps(v).encode("utf-8")
)


@app.route("/upload", methods=['POST'])
def upload_csv():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    content = file.stream.read().decode("utf-8")  # Read CSV content
    reader = csv.reader(io.StringIO(content))

    transactions = []  # Store transactions for response

    headers = next(reader, None)  # Read header row
    if headers is None:
        return jsonify({"error": "Invalid CSV file"}), 400

    for row in reader:
        if len(row) != len(headers):  # Ensure row has correct number of columns
            continue
        
        transaction_data = {
            "date": row[0],
            "transaction_narration": row[1],
            "transaction_id": row[2],
            "debit": float(row[3].replace(",","")) if row[3] else 0.0,
            "credit": float(row[4].replace(",","")) if row[4] else 0.0,
            "balance": float(row[5].replace(",","")) if row[5] else 0.0
        }

        transactions.append(transaction_data)
        producer.send(TOPIC_NAME, key=bytes(row[2], 'utf-8'), value=transaction_data)

    producer.flush()  # Ensure all messages are sent
    return jsonify({"message": "Transactions uploaded successfully"}), 200


if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5000)
