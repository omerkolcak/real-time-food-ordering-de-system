from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType
from pyspark.sql.functions import from_json, col
from pyspark.sql.functions import sum as _sum
from pyspark.sql.functions import avg as _avg

def subscribe_to_spark_topic(topic_name=None,schema=None):
    """
    Function to subscribe to given kafka topic.
    """
    return spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "localhost:9092") \
                .option("subscribe", topic_name) \
                .option("startingOffsets", "earliest") \
                .load() \
                .selectExpr("CAST(value AS STRING)") \
                .select(from_json(col("value"), schema).alias("data")) \
                .select("data.*")


if __name__ == "__main__":
    spark = (
        SparkSession
        .builder.appName("FoodOrderingSystem")
        .master("local[*]")
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.jars","C:\\Users\\Omer\\Documents\\realtime-data-streaming\\postgresql-42.7.2.jar")  # PostgreSQL driver
        .config("spark.sql.adaptive.enabled", "false")  # Disable adaptive query execution
        .getOrCreate()
    )
    # define order schema
    order_schema = StructType(
        [StructField("order_id", IntegerType(), True),
         StructField("fk_restaurant", IntegerType(), True),
         StructField("restaurant_name", StringType(), True),
         StructField("fk_user", IntegerType(), True),
         StructField("order_datetime", TimestampType(), True),
         StructField("total_cost", FloatType(), True),
         StructField("cust_restaurant_rating", FloatType(), True)
        ]
    )
    # define order item schema
    order_item_schema = StructType(
        [StructField("order_item_id", IntegerType(), True),
         StructField("fk_menu_item", IntegerType(), True),
         StructField("fk_order", IntegerType(), True),
         StructField("item_quantity", IntegerType(), True)
        ]
    )
    # order df
    order_df = subscribe_to_spark_topic(topic_name='orders_topic',schema=order_schema)
    # order item df
    order_item_df = subscribe_to_spark_topic(topic_name='order_items_topic',schema=order_item_schema)

    # typecasting
    order_df = order_df.withColumn('order_datetime', col('order_datetime').cast(TimestampType())) \
                        .withColumn('total_cost', col('total_cost').cast(FloatType())) \
                        .withColumn('cust_restaurant_rating', col('cust_restaurant_rating').cast(FloatType()))
    
    order_item_df = order_item_df.withColumn('fk_menu_item', col('fk_menu_item').cast(IntegerType())) \
                                    .withColumn('item_quantity', col('item_quantity').cast(IntegerType()))

    #order_df = order_df.withWatermark("order_datetime", "1 minute")

    restaurant_revenue = order_df.groupBy('restaurant_name').agg(_sum('total_cost').alias('total_revenue'), 
                                                                 _avg('cust_restaurant_rating').alias('avg_rate'))

    item_counts = order_item_df.groupBy('fk_menu_item').agg(_sum('item_quantity').alias('item_quantity'))

    restaurant_revenue_kafka = restaurant_revenue.selectExpr('to_json(struct(*)) AS value') \
                                                    .writeStream \
                                                    .format('kafka') \
                                                    .option('kafka.bootstrap.servers','localhost:9092') \
                                                    .option('topic','restaurant_stats') \
                                                    .option('checkpointLocation', 'C:\\Users\\Omer\\Documents\\realtime-data-streaming\\checkpoints\\checkpoint1') \
                                                    .outputMode('update') \
                                                    .start()

    item_counts_kafka = item_counts.selectExpr('to_json(struct(*)) AS value') \
                                                    .writeStream \
                                                    .format('kafka') \
                                                    .option('kafka.bootstrap.servers','localhost:9092') \
                                                    .option('topic','menu_item_stats') \
                                                    .option('checkpointLocation', 'C:\\Users\\Omer\\Documents\\realtime-data-streaming\\checkpoints\\checkpoint2') \
                                                    .outputMode('update') \
                                                    .start()

    restaurant_revenue_kafka.awaitTermination()
    item_counts_kafka.awaitTermination()