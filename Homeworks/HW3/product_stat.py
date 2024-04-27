# -*- coding: utf-8 -*-

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

from pyspark.sql.types import FloatType
from pyspark.sql.functions import regexp_replace

from pyspark.sql.window import Window


def main(argv):

    # args

    ok_dem_path = argv[1]
    city_path = argv[2]
    price_path = argv[3]
    product_path = argv[4]
    product_for_stat_path = argv[5]
    output_path = argv[5]

    spark = SparkSession.builder.getOrCreate()
    
    # read data

    ok_dem = spark.read.option("header", "true").csv(ok_dem_path, sep=';')

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

    ProductDF = (
        spark.read
            .option("header", "false")
            .option("sep", ";")
            .csv(product_path)
    )

    ProductsForStatDF = (
        spark.read
            .option("header", "false")
            .option("sep", ";")
            .csv(product_for_stat_path)
    )
    
    # cast data to DataFrame

    CityDF = (
        CityDF
        .select(
            sf.col("_c1").cast(IntegerType()).alias("city_id"),
            sf.col("_c0").alias("city")
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

    ProductDF = (
        ProductDF
        .select(
            sf.col("_c1").cast(IntegerType()).alias("product_id"),
            sf.col("_c0").alias("product")
        )
    )

    ProductsForStatDF = (
        ProductsForStatDF
        .select(
            sf.col("_c0").cast(IntegerType()).alias("product_id")
        )
    )

    ok_dem = (
        ok_dem
        .select(
            sf.col("city_name"),
            sf.col("user_cnt").cast(IntegerType()),
            sf.col("age_avg").cast(IntegerType()),
            sf.col("men_cnt").cast(IntegerType()),
            sf.col("women_cnt").cast(IntegerType()),
            sf.col("men_share").cast(FloatType()),
            sf.col("women_share").cast(FloatType())
        )
    )

    # Joins and Aggs

    PriceJoinedDF = (
        PriceDF
        .join(ProductsForStatDF, PriceDF.product_id == ProductsForStatDF.product_id, how='inner')
            .select(
                PriceDF.product_id,
                sf.col("city_id"),
                sf.col("price")
        )
    )

    vals_for_stat = ok_dem.agg(
            sf.max(sf.col("age_avg")),
            sf.min(sf.col("age_avg")),
            sf.max(sf.col("men_share")),
            sf.max(sf.col("women_share"))).collect()[0]

    CitiesForStatDF = (
        ok_dem.where( (sf.col("age_avg") == vals_for_stat[0]) | 
                        (sf.col("age_avg") == vals_for_stat[1]) |
                        (sf.col("men_share") == vals_for_stat[2]) |
                        (sf.col("women_share") == vals_for_stat[3]))
        .select(sf.col("city_name"))
    )

    CitiesForStatDF = (
        CitiesForStatDF.alias("left")
        .join(CityDF, CitiesForStatDF.city_name == CityDF.city, how='inner')
            .select(
                sf.col("city_id"),
                sf.col("city"),
        )
    )

    CitiesForStatPricesDF = (
        CitiesForStatDF.alias("left")
        .join(PriceJoinedDF, CitiesForStatDF.city_id == PriceDF.city_id, how='inner')
            .select(
                sf.col("left.city_id"),
                sf.col("city"),
                sf.col("product_id"),
                sf.col("price")
        )
    )

    CitiesForStatPricesDF = (
        CitiesForStatPricesDF.alias("left")
        .join(ProductDF, CitiesForStatPricesDF.product_id == ProductDF.product_id, how='inner')
            .select(
                sf.col("price"),
                sf.col("city").alias("city_name"),
                sf.col("product")
        )
    )

    product_stat = (
        CitiesForStatPricesDF
        .withColumn("cheap", sf.min("price").over(Window.partitionBy("city_name")))
        .withColumn("expensive", sf.max("price").over(Window.partitionBy("city_name")))
    )

    product_stat = (
        product_stat
        .select(
            sf.col("city_name"),
            sf.col("price"),
            sf.when(sf.col("price") == sf.col("cheap"), sf.col("product")).alias("cheapest_product_name"),
            sf.when(sf.col("price") == sf.col("expensive"), sf.col("product")).alias("most_expensive_product_name"),
            (sf.col("expensive") - sf.col("cheap")).alias("price_difference")
        )
        .where( (sf.col("cheapest_product_name").isNotNull()) | (sf.col("most_expensive_product_name").isNotNull()))
        .groupBy(
            sf.col("city_name")
        )
        .agg (
            sf.last(sf.col("most_expensive_product_name"), True).alias("most_expensive_product_name"),
            sf.last(sf.col("cheapest_product_name"), True).alias("cheapest_product_name"),
            sf.last("price_difference", True).alias('price_difference')
        )
    )

    # Save to hdfs

    (product_stat
     .repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path)
    )


if __name__ == '__main__':
    sys.exit(main(sys.argv))