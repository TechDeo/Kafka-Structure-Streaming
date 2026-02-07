from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka settingsfrom pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Kafka settings
KAFKA_BOOTSTRAP = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
KAFKA_TOPIC = 'my_first_topic'
KAFKA_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_SECRET = os.getenv("KAFKA_API_SECRET")

# Kafka consumer configuration
kafka_config = {
    'kafka.bootstrap.servers': KAFKA_BOOTSTRAP,
    'subscribe': KAFKA_TOPIC,
    'startingOffsets': 'earliest',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'failOnDataLoss': 'false',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_KEY}" password="{KAFKA_SECRET}";',
}

# Correct schema matching the Kafka messages
message_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("message", StringType(), True)
])

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaStreamingConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
kafka_stream = spark.readStream.format("kafka").options(**kafka_config).load()

# Parse JSON messages correctly
parsed = kafka_stream.selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), message_schema).alias("data")) \
    .select("data.*")

# Write streaming data to console
query = parsed.writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming started...")
query.awaitTermination()

KAFKA_BOOTSTRAP = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092'
KAFKA_TOPIC = 'my_first_topic'
KAFKA_KEY = os.getenv("KAFKA_API_KEY")
KAFKA_SECRET = os.getenv("KAFKA_API_SECRET")

# Kafka consumer configuration
kafka_config = {
    'kafka.bootstrap.servers': KAFKA_BOOTSTRAP,
    'subscribe': KAFKA_TOPIC,
    'startingOffsets': 'earliest',
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'failOnDataLoss': 'false',
    'kafka.sasl.jaas.config': f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_KEY}" password="{KAFKA_SECRET}";',
}

# Define schema matching the JSON messages in Kafka
message_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", IntegerType(), True)
])

# Create Spark session with Kafka support
spark = SparkSession.builder \
    .appName("KafkaStreamingConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Set Spark log level
spark.sparkContext.setLogLevel("WARN")

# Read streaming data from Kafka
kafka_stream = spark.readStream.format("kafka").options(**kafka_config).load()

# Parse JSON messages
parsed = kafka_stream.selectExpr("CAST(value AS STRING) as value") \
    .select(from_json(col("value"), message_schema).alias("data")) \
    .select("data.*")

# Absolute paths for saving files
project_root = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(project_root, "kafka_output")
checkpoint_path = os.path.join(project_root, "kafka_checkpoints")

# Console sink for debugging
console_query = parsed.writeStream \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()


# Parquet sink for persistent storage
file_query = parsed.writeStream \
    .format("parquet") \
    .option("path", "output_path") \
    .option("checkpointLocation", "checkpoint_path") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("Streaming started...")

# Await termination of both queries
console_query.awaitTermination()
file_query.awaitTermination()
