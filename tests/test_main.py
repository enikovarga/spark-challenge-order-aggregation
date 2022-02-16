import pytest

from src.main import (
    create_spark_views,
    get_num_products_purchased,
    get_agg_customer_purchase,
)


@pytest.mark.usefixtures("spark", "test_data")
def test_get_num_products_purchased(spark, test_data):
    create_spark_views(
        spark, test_data["customers"], test_data["products"], test_data["transactions"]
    )

    get_num_products_purchased(spark)

    actual = spark.table("num_products").orderBy("customer_id", "product_id").collect()

    expected = (
        spark.read.csv(test_data["num_products"], header=True, inferSchema=True)
        .orderBy("customer_id", "product_id")
        .collect()
    )

    assert actual == expected


@pytest.mark.usefixtures("spark", "test_data")
def test_get_agg_customer_purchase(spark, test_data):
    create_spark_views(
        spark, test_data["customers"], test_data["products"], test_data["transactions"]
    )

    spark.read.csv(
        test_data["num_products"], header=True, inferSchema=True
    ).createOrReplaceTempView("num_products")

    get_agg_customer_purchase(spark)

    actual = (
        spark.table("agg_customer_purchase")
        .orderBy("customer_id", "product_id")
        .collect()
    )

    expected = (
        spark.read.csv(
            test_data["agg_customer_purchase"], header=True, inferSchema=True
        )
        .orderBy("customer_id", "product_id")
        .collect()
    )

    assert actual == expected
