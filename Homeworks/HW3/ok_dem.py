# -*- coding: utf-8 -*-

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

from pyspark.sql.types import FloatType
from pyspark.sql.functions import regexp_replace


def main(argv):

    # args
    
    price_stat_path = argv[1]
    demography_path = argv[2]
    city_path = argv[3]
    city_rs_path = argv[4]
    price_path = argv[5]
    current_dt = argv[6]
    output_path = argv[7]

    spark = SparkSession.builder.getOrCreate()

    # read data

    UserDF = (
        spark.read
            .option("header", "false")
            .option("sep", "\t")
            .csv(demography_path)
    )

    CityRsDF = (
        spark.read
            .option("header", "false")
            .option("sep", "\t")
            .csv(city_rs_path)
    )   

    CityDF = (
        spark.read
            .option("header", "false")
            .option("sep", ";")
            .csv(city_path)
    )

    PriceDF = (
        spark.read
            .option("header", "false")
            .option("sep", ";")
            .csv(price_path)
    )

    PriceStatDF = spark.read.option("header", "true").csv(price_stat_path, sep=';')

    # cast data to DataFrame

    CityDF = (
        CityDF
        .select(
            sf.col("_c1").cast(IntegerType()).alias("city_id"),
            sf.col("_c0").alias("city")
        )
    )

    UserDF = (
        UserDF
        .select(
            sf.col("_c0").cast(IntegerType()).alias("id"),
            sf.col("_c1").cast(LongType()).alias("create_date"),
            sf.col("_c2").cast(LongType()).alias("birth_date"),
            sf.col("_c3").cast(ByteType()).alias("gender"),
            sf.col("_c4").cast(IntegerType()).alias("id_country"),
            sf.col("_c5").cast(IntegerType()).alias("id_location"),
            sf.col("_c6").cast(IntegerType()).alias("id_region"),
        )
    )
    
    CityRsDF = (
        CityRsDF
        .select(
            sf.col("_c0").cast(IntegerType()).alias("ok_city_id"),
            sf.col("_c1").cast(IntegerType()).alias("rs_city_id")
        )
    )

    PriceDF = PriceDF.withColumn('_c2', regexp_replace('_c2', ',', '.'))

    PriceDF = (
        PriceDF
        .select(
            sf.col("_c0").cast(IntegerType()).alias("city_id"),
            sf.col("_c1").cast(IntegerType()).alias("product_id"),
            sf.col("_c2").cast(FloatType()).alias("price")
        )
    )

    # Joins and Aggs

    PricePriceStatDF = (
        PriceDF.alias("left")
        .join(PriceStatDF, PriceDF.product_id == PriceStatDF.product_id, how='inner')
            .select(
                sf.col("left.product_id"),
                sf.col("city_id"),
                sf.col("price"),
                sf.col("avg_price")
            )
    )

    CitiesForStatDF = (
        PricePriceStatDF.select(sf.col("city_id").alias("rs_city_id")).where(sf.col("price") > sf.col("avg_price")).distinct()
    )

    CitiesForStatDF = (
         CitiesForStatDF.alias("left")
            .join(CityDF, CitiesForStatDF.rs_city_id == CityDF.city_id, how='inner')
            .select(
                sf.col("left.rs_city_id"),
                sf.col("city"),
            )
    )

    CitiesForStatDF = (
         CitiesForStatDF.alias("left")
        .join(CityRsDF, CitiesForStatDF.rs_city_id == CityRsDF.rs_city_id, how='inner')
        .select(
            sf.col("left.rs_city_id"),
            sf.col("city"),
            sf.col("ok_city_id")
        )
    )

    UserWithCityDF = (
        UserDF.alias("left")
        .join(CitiesForStatDF, UserDF.id_location == CitiesForStatDF.ok_city_id, how='inner')
            .select(
                sf.col("city"),
                sf.col("gender"),
                sf.col("birth_date"),
        )
    )

    ok_dem = (
        UserWithCityDF.groupBy(sf.col("city"))
        .agg(
            sf.count(sf.col("birth_date")).alias("user_cnt"),
            sf.avg(sf.datediff(sf.lit(current_dt), sf.from_unixtime(sf.col("birth_date") * 24 * 3600)) / 365.25).cast(IntegerType()).alias("age_avg"),
            sf.count(sf.when(sf.col("gender") == 1, True)).alias("men_cnt"),
            sf.count(sf.when(sf.col("gender") == 2, True)).alias("women_cnt"),
        )
        .select(
            sf.col("city").alias("city_name"),
            sf.col("user_cnt"),
            sf.col("age_avg"),
            sf.col("men_cnt"),
            sf.col("women_cnt"),
            sf.round((sf.col("men_cnt") / sf.col("user_cnt")), 2).alias("men_share"),
            sf.round((sf.col("women_cnt") / sf.col("user_cnt")), 2).alias("women_share")
        )
        .orderBy(sf.col("user_cnt"), ascending=False)
    )

    # Save to hdfs

    (ok_dem
     .repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path)
    )


if __name__ == '__main__':
    sys.exit(main(sys.argv))