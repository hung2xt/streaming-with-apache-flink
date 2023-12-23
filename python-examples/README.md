# PyFlink Kafka to MySQL Tutorial

This tutorial demonstrates how to read data from a Kafka topic and write it to a MySQL database using Apache Flink's Python API, PyFlink.

## Prerequisites

- Apache Flink
- Apache Kafka
- MySQL Server
- Python 3.x
- PyFlink installed (Please visit the `requirements.txt`)

## Setup

### Kafka

1. **Start Kafka Broker**: Ensure your Kafka broker is running.
2. **Create Kafka Topic**: Create a topic named `transactions`.

### MySQL

1. **Start MySQL Server**: Ensure your MySQL server is running.
2. **Create MySQL Table**: Use the following SQL to create the `transactions` table in your MySQL database:

   ```sql
   CREATE TABLE financial_transactions (
       transactionId VARCHAR(255),
       productId VARCHAR(255),
       productName VARCHAR(255),
       productCategory VARCHAR(255),
       productPrice FLOAT,
       productQuantity INT,
       productBrand VARCHAR(255),
       currency VARCHAR(255),
       customerId VARCHAR(255),
       transactionDate VARCHAR(255),
       paymentMethod VARCHAR(255),
       totalAmount FLOAT
   );
   ```

## Running the Flink Job

The Flink job consists of two main components:

1. **Kafka Source**: Reads data from the Kafka topic.
2. **MySQL Sink**: Writes data to the MySQL table.

### Kafka Source
If you don't use Docker for the installation of Kafka. You might install Kafka on your machine and start your Kafka cluster. You might visit [here](https://kafka.apache.org/quickstart)

```bash
bash StartKafka.sh
```

The Kafka source is set up to read JSON-formatted messages from the `transactions` topic. Messages should follow this format:

```json
{
  "transactionId": "string",
  "productId": "string",
  "productName": "string",
  "productCategory": "string",
  "productPrice": number,
  "productQuantity": number,
  "productBrand": "string",
  "currency": "string",
  "customerId": "string",
  "transactionDate": "string",  // format: "YYYY-MM-DDTHH:MM:SS.ssssss"
  "paymentMethod": "string",
  "totalAmount": number
}

```

### MySQL Sink

The MySQL sink writes the incoming records to the `transactions` table in the MySQL database.

### Install drivers for Apache Flink

```bash
wget wget https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.15.0/flink-sql-connector-kafka-1.15.0.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.1.1-1.17/flink-connector-jdbc-3.1.1-1.17.jar 
wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.29/mysql-connector-java-8.0.29.jar
```

### Running the Script

1. Navigate to the directory containing the PyFlink script.
2. Run the script:

   ```bash
   python KafkaSinkMySQL.py
   ```

   Replace `KafkaSinkMySQL.py` with the name of your Python script.

### Clean up

```bash
bash StopKafka.sh
```
## Notes

- Ensure the MySQL JDBC driver (`/mysql-connector-java-8.0.29.jar`) is available in your lib directory.
- Adjust the Kafka and MySQL connection properties in the script to match your setup.



