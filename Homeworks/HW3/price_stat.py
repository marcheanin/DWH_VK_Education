# -*- coding: utf-8 -*-

import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as sf

from pyspark.sql.types import FloatType
from pyspark.sql.functions import regexp_replace


def main(argv):

    # args
    
    price_path = argv[1]
    products_for_stat_path = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()

    # read data

    PriceDF = (
        spark.read
            .option("header", "false")
            .option("sep", ";")
            .csv(price_path)
    )

    ProductsForStatDF = (
        spark.read
            .option("header", "false")
            .option("sep", ";")
            .csv(products_for_stat_path)
    )
    
    # cast to DataFrame
    
    PriceDF = PriceDF.withColumn('_c2', regexp_replace('_c2', ',', '.'))

  

    PriceDF = (
        PriceDF
        .select(
            sf.col("_c0").cast(IntegerType()).alias("city_id"),
            sf.col("_c1").cast(IntegerType()).alias("product_id"),
            sf.col("_c2").cast(FloatType()).alias("price")
        )
    )
    
    ProductsForStatDF = (
        ProductsForStatDF
        .select(
            sf.col("_c0").cast(IntegerType()).alias("product_id")
        )
    )

    # Joins an Aggs

    PriceJoinedDF = (
        PriceDF
        .join(ProductsForStatDF, PriceDF.product_id == ProductsForStatDF.product_id, how='inner')
            .select(
                PriceDF.product_id,
                sf.col("city_id"),
                sf.col("price")
        )
    )

    PriceStatDF = (
        PriceJoinedDF
            .groupBy("product_id")
            .agg(sf.max("price").alias("max_price"),
                sf.round(sf.avg("price"), 2).alias("avg_price"),
                sf.min("price").alias("min_price")
            )
    )

    # Save to hdfs

    (PriceStatDF
     .repartition(1)
     .write
     .mode("overwrite")
     .option("header", "true")
     .option("sep", ";")
     .csv(output_path)
    )


if __name__ == '__main__':
    sys.exit(main(sys.argv))