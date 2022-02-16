import argparse

from pyspark.sql import SparkSession


def create_spark_session():
    spark = SparkSession.builder.master("local[2]").appName("DataTest").getOrCreate()

    return spark


def create_spark_views(
    spark: SparkSession,
    customers_location: str,
    products_location: str,
    transactions_location: str,
):
    spark.read.csv(
        customers_location, header=True, inferSchema=True
    ).createOrReplaceTempView("customers")
    spark.read.csv(
        products_location, header=True, inferSchema=True
    ).createOrReplaceTempView("products")
    spark.read.json(transactions_location).createOrReplaceTempView("transactions")


def get_num_products_purchased(spark):
    spark.sql(
        """SELECT *,
                        COUNT(*) AS purchase_count
                FROM
                    (SELECT customer_id, 
                        explode(basket.product_id) AS product_id
                    FROM transactions)
                GROUP BY
                    customer_id,
                    product_id"""
    ).createOrReplaceTempView("num_products")


def get_agg_customer_purchase(spark):
    spark.sql(
        """ SELECT 
                        ac.customer_id,
                        c.loyalty_score,
                        ac.product_id,
                        p.product_category,
                        ac.purchase_count
                    FROM num_products ac 
                    LEFT JOIN customers c 
                    ON c.customer_id = ac.customer_id
                    LEFT JOIN products p 
                    ON p.product_id = ac.product_id
    """
    ).createOrReplaceTempView("agg_customer_purchase")


def main(args):
    spark = create_spark_session()

    create_spark_views(
        spark,
        args["customers_location"],
        args["products_location"],
        args["transactions_location"],
    )
    spark.table("transactions").show(truncate=False)

    get_num_products_purchased(spark)

    get_agg_customer_purchase(spark)

    spark.table("agg_customer_purchase").show(10, truncate=False)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="DataTest")
    parser.add_argument(
        "--customers_location",
        required=False,
        default="./input_data/starter/customers.csv",
    )
    parser.add_argument(
        "--products_location",
        required=False,
        default="./input_data/starter/products.csv",
    )
    parser.add_argument(
        "--transactions_location",
        required=False,
        default="./input_data/starter/transactions/",
    )
    parser.add_argument(
        "--output_location", required=False, default="./output_data/outputs/"
    )
    args = vars(parser.parse_args())

    main(args)
