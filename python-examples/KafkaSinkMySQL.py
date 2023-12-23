import logging
import sys
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common import Types, Row
from pyflink.table import DataTypes
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer, FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowSerializationSchema, JsonRowDeserializationSchema
from pyflink.datastream.formats.json import JsonRowDeserializationSchema

# Make sure that the Kafka cluster is started and the topic 'test_json_topic' is
# created before executing this job.


def read_from_kafka_write_to_mysql(env):
    # Define the schema with custom field names
    row_type_info = RowTypeInfo(
        [
            Types.STRING(),  # transactionId
            Types.STRING(),  # productId
            Types.STRING(),  # productName
            Types.STRING(),  # productCategory
            Types.FLOAT(),   # productPrice
            Types.INT(),     # productQuantity
            Types.STRING(),  # productBrand
            Types.STRING(),  # currency
            Types.STRING(),  # customerId
            Types.STRING(),  # transactionDate
            Types.STRING(),  # paymentMethod
            Types.FLOAT()    # totalAmount
        ],
        [
            "transactionId",
            "productId",
            "productName",
            "productCategory",
            "productPrice",
            "productQuantity",
            "productBrand",
            "currency",
            "customerId",
            "transactionDate",
            "paymentMethod",
            "totalAmount"
        ]
    )

    deserialization_schema = JsonRowDeserializationSchema.Builder() \
        .type_info(row_type_info) \
        .build()

    kafka_consumer = FlinkKafkaConsumer(
        topics='transactions',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092'}
    )
    kafka_consumer.set_start_from_earliest()

    jdbc_sink = JdbcSink.sink(
        sql = "INSERT INTO financial_transactions (transactionId, productId, productName, productCategory, productPrice, productQuantity, productBrand, currency, customerId, transactionDate, paymentMethod, totalAmount) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        type_info = Types.ROW(
        [
            Types.STRING(),  # transactionId
            Types.STRING(),  # productId
            Types.STRING(),  # productName
            Types.STRING(),  # productCategory
            Types.FLOAT(),   # productPrice
            Types.INT(),     # productQuantity
            Types.STRING(),  # productBrand
            Types.STRING(),  # currency
            Types.STRING(),  # customerId
            Types.STRING(),  # transactionDate
            Types.STRING(),  # paymentMethod
            Types.FLOAT()    # totalAmount
        ]
    ),
    jdbc_execution_options = JdbcExecutionOptions.builder()
            .with_batch_size(1000) \
            .with_batch_interval_ms(200) \
            .with_max_retries(3) \
            .build(),
    jdbc_connection_options = JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .with_url("jdbc:mysql://localhost:3306/your_database") \
            .with_driver_name("com.mysql.cj.jdbc.Driver") 
            .with_user_name("your_user_name") \
            .with_password("your_password") \
            .build()
        

    )
    env.add_source(kafka_consumer).add_sink(jdbc_sink)
    env.execute()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment() 
    #
    env.add_jars("file:///Users/jar-files/flink-sql-connector-kafka-3.0.2-1.18.jar")
    env.add_jars("file:///Users/jar-files/flink-connector-jdbc-3.1.1-1.17.jar")
    env.add_jars("file:///Users//jar-files/mysql-connector-java-8.0.29.jar")
    # print("start writing data to kafka")
    # write_to_kafka(env)

    print("start reading data from kafka and writing to mysql")
    read_from_kafka_write_to_mysql(env)



