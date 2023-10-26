from confluent_kafka import Consumer, KafkaError
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "dis"
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import datetime
import uuid


spark = SparkSession.builder \
    .appName("Spark Kafka Streaming Data Pipeline") \
    .config("spark.cassandra.connection.host", "172.18.0.5") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.driver.host", "localhost") \
    .getOrCreate()



consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.auto.offset.store': False,
        'enable.partition.eof': False
    })

topic = "fridge"

consumer.subscribe([topic])

# set up schema here 
# Define the schema for the Kafka message
fridgeSchema = StructType([
    StructField("uuid", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("temp_condition", StringType(), True),
    StructField("attack", StringType(), True),
    StructField("label", IntegerType(), True)
])

# # Create a Kafka DataFrame for streaming
sparkFridgeDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.18.0.4:9092") \
    .option("subscribe", "fridge") \
    .option("startingOffsets", "earliest") \
    .load()
    

timestamp = msg.timestamp()[1] // 1000.0
date_time = datetime.fromtimestamp(timestamp)
# uuid_udf = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()

uuid_str = str(uuid.uuid4())
print(uuid_str)

new_row = Row(
    uuid=uuid_str,
    timestamp= date_time,
    temperature=pdu.temperature,
    temp_condition=pdu.condition.decode('utf-8'),
    attack=pdu.attack.decode('utf-8'),
    label=pdu.label)

# Append the new Row to the DataFrame
appended_df = sparkFridgeDF.union(spark.createDataFrame([new_row], fridgeSchema))

#  Show the result
# appended_df.show()

# Output to Console
console_query = appended_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()




{"type": "struct", "fields": [
    {"name": "uuid", "type": "string", "nullable": true, "metadata": {}}, 
    {"name": "timestamp", "type": "timestamp", "nullable": true, "metadata": {}}, 
    {"name": "temperature", "type": "double", "nullable": true, "metadata": {}}, 
    {"name": "temp_condition", "type": "string", "nullable": true, "metadata": {}}, 
    {"name": "attack", "type": "string", "nullable": true, "metadata": {}}, 
    {"name": "label", "type": "integer", "nullable": true, "metadata": {}}]
}

fridge_schema_json = '''
{
  "type": "struct",
  "fields": [
    {"name": "uuid", "type": "string", "nullable": true},
    {"name": "timestamp", "type": "timestamp", "nullable": true},
    {"name": "temperature", "type": "double", "nullable": true},
    {"name": "temp_condition", "type": "string", "nullable": true},
    {"name": "attack", "type": "string", "nullable": true},
    {"name": "label", "type": "integer", "nullable": true}
  ]
}
'''