from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import pipelines as dp


@dp.table(
    name="transportation.gold.DIM_CUSTOMER",
    comment="Flat customer dimension with denormalized geography (SCD1).",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def dim_customer():
    customer = spark.read.table("transportation.silver.BL_3NF_CE_CUSTOMER")
    city = (
        spark.read.table("transportation.silver.BL_3NF_CE_CITY")
        .select("city_id", "state_id", "city_name")
    )
    state = (
        spark.read.table("transportation.silver.BL_3NF_CE_STATE")
        .select("state_id", "region_id", "state_name")
    )
    region = (
        spark.read.table("transportation.silver.BL_3NF_CE_REGION")
        .select("region_id", "country_id", "region_name")
    )
    country = (
        spark.read.table("transportation.silver.BL_3NF_CE_COUNTRY")
        .select("country_id", "country_name")
    )

    return (
        customer
        .join(city, on="city_id", how="left")
        .join(state, on="state_id", how="left")
        .join(region, on="region_id", how="left")
        .join(country, on="country_id", how="left")
        .withColumn("customer_sk", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "customer_sk",
            "customer_id",
            "customer_src_id",
            "first_name",
            "last_name",
            "gender",
            "age",
            "email",
            "phone_number",
            "postal_code",
            "city_name",
            "state_name",
            "region_name",
            "country_name",
            "source_system",
            "insert_dt",
            "update_dt",
        )
    )


@dp.table(
    name="transportation.gold.DIM_EMPLOYEE",
    comment="Employee dimension preserving SCD2 history from silver.",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def dim_employee():
    emp = spark.read.table("transportation.silver.BL_3NF_EMPLOYEE_SCD2")

    return (
        emp
        .withColumn("employee_sk", F.monotonically_increasing_id())
        .withColumn("source_system", F.lit("offline"))
        .withColumnRenamed("__START_AT", "start_at")
        .withColumnRenamed("__END_AT", "end_at")
        .withColumn("is_current", F.col("end_at").isNull())
        .select(
            "employee_sk",
            "employee_src_id",
            "first_name",
            "last_name",
            "gender",
            "monthly_salary",
            "store_src_id",
            "start_at",
            "end_at",
            "is_current",
            "source_system",
        )
    )


@dp.table(
    name="transportation.gold.DIM_STORE",
    comment="Flat store dimension with denormalized geography.",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def dim_store():
    store = spark.read.table("transportation.silver.BL_3NF_CE_STORE")
    city = (
        spark.read.table("transportation.silver.BL_3NF_CE_CITY")
        .select("city_id", "state_id", "city_name")
    )
    state = (
        spark.read.table("transportation.silver.BL_3NF_CE_STATE")
        .select("state_id", "region_id", "state_name")
    )
    region = (
        spark.read.table("transportation.silver.BL_3NF_CE_REGION")
        .select("region_id", "country_id", "region_name")
    )
    country = (
        spark.read.table("transportation.silver.BL_3NF_CE_COUNTRY")
        .select("country_id", "country_name")
    )

    return (
        store
        .join(city, on="city_id", how="left")
        .join(state, on="state_id", how="left")
        .join(region, on="region_id", how="left")
        .join(country, on="country_id", how="left")
        .withColumn("store_sk", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "store_sk",
            "store_id",
            "store_src_id",
            "store_name",
            "city_name",
            "state_name",
            "region_name",
            "country_name",
            "source_system",
            "insert_dt",
            "update_dt",
        )
    )


@dp.table(
    name="transportation.gold.DIM_PRODUCT",
    comment="Flat product dimension with denormalized category hierarchy.",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def dim_product():
    product = spark.read.table("transportation.silver.BL_3NF_CE_PRODUCTS").drop("sub_category_name")
    sub_cat = (
        spark.read.table("transportation.silver.BL_3NF_CE_SUB_CATEGORIES")
        .select("sub_category_id", "category_id", "sub_category_name")
    )
    cat = (
        spark.read.table("transportation.silver.BL_3NF_CE_CATEGORIES")
        .select("category_id", "category_name")
    )

    return (
        product
        .join(sub_cat, on="sub_category_id", how="left")
        .join(cat, on="category_id", how="left")
        .withColumn("product_sk", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "product_sk",
            "product_id",
            "product_src_id",
            "product_name",
            "sub_category_name",
            "category_name",
            "source_system",
            "insert_dt",
            "update_dt",
        )
    )

@dp.table(
    name="transportation.gold.DIM_PAYMENT_METHOD",
    comment="Payment method dimension (passthrough of the silver dictionary).",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def dim_payment_method():
    pm = spark.read.table("transportation.silver.BL_3NF_CE_PAYMENT_METHODS")
    return (
        pm
        .withColumn("payment_method_sk", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "payment_method_sk",
            "method_id",
            "payment_method",
            "payment_provider",
            "source_system",
            "insert_dt",
            "update_dt",
        )
    )

@dp.table(
    name="transportation.gold.DIM_DATE",
    comment="Date dimension generated from the order_date range.",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def dim_date():
    bounds = (
        spark.read.table("transportation.silver.BL_3NF_CE_ORDERS")
        .agg(
            F.min(F.to_date("order_date")).alias("min_d"),
            F.max(F.to_date("order_date")).alias("max_d"),
        )
    )

    sequence_df = bounds.select(
        F.explode(
            F.sequence(F.col("min_d"), F.col("max_d"), F.expr("interval 1 day"))
        ).alias("full_date")
    )

    return (
        sequence_df
        .withColumn("date_sk", F.date_format(F.col("full_date"), "yyyyMMdd").cast("int"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("quarter", F.quarter("full_date"))
        .withColumn("year", F.year("full_date"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
        .select(
            "date_sk",
            "full_date",
            "day",
            "month",
            "quarter",
            "year",
            "day_of_week",
            "is_weekend",
        )
    )


@dp.table(
    name="transportation.gold.FACT_SALES",
    comment="Sales fact at order-item grain for the sales star schema.",
    table_properties={
        "quality": "gold",
        "layer": "gold_star"
    }
)
def fact_sales():
    items = spark.read.table("transportation.silver.BL_3NF_CE_ORDERS_ITEMS")
    orders = (
        spark.read.table("transportation.silver.BL_3NF_CE_ORDERS")
        .select(
            "order_id",
            "order_src_id",
            "customer_id",
            "store_id",
            "employee_src_id",
            "order_date",
            "source_system",
            "source_entity",
        )
    )
    payment = spark.read.table("transportation.silver.BL_3NF_CE_PAYMENT")

    w = Window.partitionBy("order_id").orderBy(F.col("payment_id").asc())
    payment_per_order = (
        payment
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select("order_id", "method_id", "payment_amount")
    )

    dc = (
        spark.read.table("transportation.gold.DIM_CUSTOMER")
        .select("customer_sk", "customer_id")
    )
    de = (
        spark.read.table("transportation.gold.DIM_EMPLOYEE")
        .filter(F.col("is_current"))
        .select("employee_sk", "employee_src_id")
        .dropDuplicates(["employee_src_id"])
    )
    ds = (
        spark.read.table("transportation.gold.DIM_STORE")
        .select("store_sk", "store_id")
    )
    dp_prod = (
        spark.read.table("transportation.gold.DIM_PRODUCT")
        .select("product_sk", "product_id")
    )
    dpm = (
        spark.read.table("transportation.gold.DIM_PAYMENT_METHOD")
        .select("payment_method_sk", "method_id")
    )
    dd = (
        spark.read.table("transportation.gold.DIM_DATE")
        .select("date_sk", "full_date")
    )

    df = (
        items
        .join(orders, on="order_id", how="left")
        .join(payment_per_order, on="order_id", how="left")
        .withColumn("full_date", F.to_date("order_date"))
        .join(dc, on="customer_id", how="left")
        .join(de, on="employee_src_id", how="left")
        .join(ds, on="store_id", how="left")
        .join(dp_prod, on="product_id", how="left")
        .join(dpm, on="method_id", how="left")
        .join(dd, on="full_date", how="left")
        .withColumn(
            "sale_sk",
            F.md5(F.concat(F.col("order_src_id"), F.col("product_id").cast("string")))
        )
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
    )

    return (
        df.select(
            F.col("sale_sk"),
            F.col("order_src_id").alias("order_id"),
            F.col("order_item_id"),
            F.col("customer_sk"),
            F.col("employee_sk"),
            F.col("store_sk"),
            F.col("product_sk"),
            F.col("payment_method_sk"),
            F.col("date_sk").alias("order_date_sk"),
            F.col("sales"),
            F.col("quantity"),
            F.col("discount"),
            F.col("profit"),
            F.col("cost"),
            F.col("payment_amount"),
            F.col("source_system"),
            F.col("source_entity"),
            F.col("insert_dt"),
            F.col("update_dt"),
        )
    )