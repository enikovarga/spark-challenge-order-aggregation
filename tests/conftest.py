import shutil
from tempfile import mkdtemp

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[2]").appName("SparkChallenge").getOrCreate()
    )

    return spark


@pytest.fixture(scope="session")
def test_data():
    return {
        "customers": "tests/test_data/customers.csv",
        "products": "tests/test_data/products.csv",
        "transactions": "tests/test_data/transactions.json",
        "num_products": "tests/test_data/num_products.csv",
        "agg_customer_purchase": "tests/test_data/agg_customer_purchase.csv",
    }
