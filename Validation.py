from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("DataProcessing").getOrCreate()
ssc = StreamingContext(spark, 10)  # Create streaming context with 10-second window

# Define function to process incoming data
def process_data(rdd):
  valid_data = rdd.filter(lambda x: x['timestamp'] is not None and x['user_id'])
  return valid_data

# Subscribe to Kafka topic and process data stream
data_stream = ssc.textFileStream("kafka-topic")
processed_stream = data_stream.map(lambda line: json.loads(line)) \
                              .transform(process_data)

# Load user profile data from external source (replace with your logic)
user_profiles_df = spark.read.parquet("user_profiles.parquet")

# Join processed stream with user profiles for enrichment
enriched_stream = processed_stream.join(user_profiles_df, on='user_id', how='left')

# Assuming click and conversion DataFrames exist
clicks_df = spark.sql("SELECT * FROM clicks_table")
conversions_df = spark.sql("SELECT * FROM conversions_table")

# Join enriched stream with clicks and conversions for correlation
correlated_data = enriched_stream.join(clicks_df, on=['user_id', 'timestamp'], how='left') \
                                  .join(conversions_df, on=['user_id', 'timestamp'], how='left')

# Calculate KPIs like CTR and conversion rate
correlated_data = correlated_data.withColumn('CTR', correlated_data['clicks'] / correlated_data['impressions']) \
                                 .withColumn('conversion_rate', correlated_data['conversions'] / correlated_data['impressions'])

# Write correlated data to parquet files in S3 landing zone
correlated_data.write.parquet("s3://your-bucket/landing-zone/impressions")


# Assuming processed data is stored in Spark DataFrames:
impressions_df = spark.sql("SELECT * FROM impressions_table")
clicks_df = spark.sql("SELECT * FROM clicks_table")

# Join DataFrames based on user ID and timestamp for correlation
correlated_data = impressions_df.join(clicks_df, on=['user_id', 'timestamp'], how='inner')

# Calculate KPIs like click-through rate (CTR)
correlated_data = correlated_data.withColumn('CTR', correlated_data['clicks'] / correlated_data['impressions'])

# Additional analysis (optional):
# You can perform further analysis on the correlated data like:
# * Group by campaign ID and calculate average CTR
# * Filter data by specific timeframe
# * Explore user behavior based on additional features

# Write data to parquet for data landing zone
correlated_data.write.parquet("s3://your-bucket/landing-zone/impressions")

# Define a function to load data into Redshift (replace with your actual logic)
def load_to_redshift(data):
  # Use a Redshift connector library (e.g., pyspark.sql.jdbc) to connect and write data
  data.write.jdbc(url="jdbc:redshift://...", table="impressions", mode="append")

# Example usage (assuming Airflow integration)
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(dag_id='data_to_redshift', schedule_interval=None) as dag:

  def read_and_load():
    landing_zone_data = spark.read.parquet("s3://your-bucket/landing-zone/impressions")
    load_to_redshift(landing_zone_data)

  load_data_task = PythonOperator(
      task_id='load_data',
      python_callable=read_and_load,
      provide_context=True
  )

load_data_task


ssc.start()
ssc.awaitTermination()
