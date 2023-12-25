import logging
import sys

from pyflink.datastream.connectors.elasticsearch import Elasticsearch6SinkBuilder, \
    Elasticsearch7SinkBuilder, FlushBackoffType, ElasticsearchEmitter

from pyflink.common import Types, Row   
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types, RowTypeInfo
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.formats.json import JsonRowDeserializationSchema
from pyflink.table import Row
from pyflink.datastream.functions import MapFunction
from pyflink.datastream.functions import SinkFunction

def create_kafka_source(env):
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

    return FlinkKafkaConsumer(
        topics='financial_transactions',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092'}
    ).set_start_from_earliest(), row_type_info
    

def create_es_sink_from_kafka(env):
    ELASTICSEARCH_SQL_CONNECTOR_PATH = \
        'file:///Users/jar-files/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar' #Config your jar file here
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH)

    return Elasticsearch7SinkBuilder() \
        .set_emitter(ElasticsearchEmitter.static_index('transaction_nx')) \
        .set_hosts(['localhost:9200']) \
        .set_bulk_flush_max_actions(1) \
        .set_bulk_flush_max_size_mb(2) \
        .set_bulk_flush_interval(1000) \
        .set_bulk_flush_backoff_strategy(FlushBackoffType.CONSTANT, 3, 3000) \
        .set_connection_request_timeout(30000) \
        .set_connection_timeout(31000) \
        .set_socket_timeout(32000) \
        .build()

class ConvertRowToDict(MapFunction):
    def __init__(self, field_names):
        self.field_names = field_names

    def map(self, row):
        return {self.field_names[i]: str(row[i]) for i in range(len(self.field_names))}


    
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env.add_jars("file:///Users/jar-files/flink-sql-connector-kafka-3.0.2-1.18.jar") #Config your jar file here

    kafka_source, row_type_info = create_kafka_source(env)

    # Create Elasticsearch Sink
    es_sink = create_es_sink_from_kafka(env)

    # Data processing and sink setup
    stream = env.add_source(kafka_source)

    field_names = [
    "transactionId", "productId", "productName", "productCategory",
    "productPrice", "productQuantity", "productBrand", "currency",
    "customerId", "transactionDate", "paymentMethod", "totalAmount"
]
    convert_function = ConvertRowToDict(field_names)

    processed_stream = stream.map(
        convert_function, 
        Types.MAP(Types.STRING(), Types.STRING())
    )

    processed_stream.print()
    
    processed_stream.sink_to(es_sink).name('es sink')
    
    env.execute('kafka-es')
    