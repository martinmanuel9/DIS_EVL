from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Define the schema for the DataFrame
schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("altitude", DoubleType(), True),
    StructField("yaw", DoubleType(), True),
    StructField("pitch", DoubleType(), True),
    StructField("roll", DoubleType(), True),
    StructField("attack", StringType(), True),
    StructField("label", StringType(), True)
])

# Create a SparkSession
spark = SparkSession.builder.appName("PDU to DataFrame").getOrCreate()

# Create an empty list to store the PDU data
pdu_data = []

# Loop through the PDUs and extract the required data
while True:
    msg = consumer.poll(timeout=1.0)
    if msg is None:
        continue
    if msg.error():
        logging.error(f"Consumer error: {msg.error()}")
    else:
        message = msg.value()
        if isinstance(message, bytes):
            try:
                pdu = createPdu(message)
                if pdu.pduType == 1:
                    gps = GPS()
                    loc = (pdu.entityLocation.x,
                           pdu.entityLocation.y,
                           pdu.entityLocation.z,
                           pdu.entityOrientation.psi,
                           pdu.entityOrientation.theta,
                           pdu.entityOrientation.phi)
                    gps.update(loc)
                    body = gps.ecef2llarpy(*loc)
                    pdu_data.append((
                        pdu.entityID.entityID,
                        body[0],
                        body[1],
                        body[2],
                        body[3],
                        body[4],
                        body[5],
                        pdu.attack.decode('utf-8'),
                        pdu.label
                    ))
            except UnicodeDecodeError as e:
                print("UnicodeDecodeError: ", e)
        else:
            logging.error("Received message is not a byte-like object.")

# Create a PySpark DataFrame from the PDU data and schema
df = spark.createDataFrame(pdu_data, schema)

# Show the DataFrame
df.show()
