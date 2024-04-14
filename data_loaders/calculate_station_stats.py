if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(*args, **kwargs):
    """
    Template code for loading data from any source.

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    import os
    from datetime import datetime, timedelta

    import pyspark.sql.functions as F
    import pyspark.sql.types as T
    from pyspark.sql import SparkSession, Window

    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
    KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "station_status")

    spark = SparkSession.builder.master(os.getenv('SPARK_MASTER_HOST', 'local')).getOrCreate()

    schema = T.StructType(
        [
            T.StructField(
                "data",
                T.StructType(
                    [
                        T.StructField(
                            "stations",
                            T.ArrayType(
                                T.StructType(
                                    [
                                        T.StructField(
                                            "is_installed", T.BooleanType(), nullable=True
                                        ),
                                        T.StructField(
                                            "is_renting", T.BooleanType(), nullable=True
                                        ),
                                        T.StructField(
                                            "is_returning", T.BooleanType(), nullable=True
                                        ),
                                        T.StructField(
                                            "last_reported", T.LongType(), nullable=True
                                        ),
                                        T.StructField(
                                            "num_bikes_available",
                                            T.LongType(),
                                            nullable=True,
                                        ),
                                        T.StructField(
                                            "num_docks_available",
                                            T.LongType(),
                                            nullable=True,
                                        ),
                                        T.StructField(
                                            "station_id", T.StringType(), nullable=True
                                        ),
                                    ]
                                )
                            ),
                        )
                    ]
                ),
            ),
            T.StructField("last_updated", T.LongType(), nullable=True),
            T.StructField("ttl", T.LongType(), nullable=True),
            T.StructField("version", T.StringType(), nullable=True),
        ]
    )

    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .load()
    )

    json_df = df.selectExpr("cast(value as string) as value")

    json_expanded_df = json_df.withColumn(
        "value", F.from_json(json_df["value"], schema)
    ).select("value.*")

    exploded_df = (
        json_expanded_df.select("last_updated", "data")
        .withColumn("station_data", F.explode("data.stations"))
        .select(
            "last_updated",
            F.col("station_data.num_bikes_available").alias("num_bikes_available"),
            F.col("station_data.station_id").alias("station_id"),
        )
    )

    one_hour_ago = int((datetime.now() - timedelta(hours=1)).timestamp())
    filtered_df = exploded_df.filter(F.col("last_updated") >= one_hour_ago)

    @F.udf(T.IntegerType())
    def calculate_difference(current_val, prev_val):
        if prev_val is None:
            return None
        else:
            return current_val - prev_val


    # Calculate the bike difference
    windowSpec = Window.partitionBy("station_id").orderBy("last_updated")
    diff_df = filtered_df.withColumn(
        "prev_num_bikes_available", F.lag("num_bikes_available").over(windowSpec)
    )
    diff_df = diff_df.withColumn(
        "bike_difference",
        calculate_difference(F.col("num_bikes_available"), F.col("prev_num_bikes_available")),
    )

    # Calculate num_rentals (absolute value of negative bike differences) and num_returns
    rentals_returns_df = diff_df.groupBy("station_id").agg(
        F.min("last_updated").alias("first_timestamp"),
        F.max("last_updated").alias("last_timestamp"),
        F.sum(F.when(F.col("bike_difference") < 0, F.abs(F.col("bike_difference")))).alias(
            "num_rentals"
        ),
        F.sum(F.when(F.col("bike_difference") > 0, F.col("bike_difference"))).alias(
            "num_returns"
        ),
    )

    return rentals_returns_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
