from pyspark.sql import functions as F
from pyspark import pipelines as dp


@dp.table(
    name = "transportation.silver.BL_3NF_CE_CATEGORIES",
    comment = "Table for online and offline categories",
    table_properties = {
        "quality": "silver",
        "layer": "silver_3nf"
    }
)
def bl_3nf_ce_categories():
    df_offline = (
        spark.read.table("transportation.bronze.offline_bronze")
        .select(F.col("Category").alias("category_name"))
        .withColumn("source_system", F.lit("offline"))
    )

    df_online = (
        spark.read.table("transportation.bronze.online_bronze")
        .select(F.col("Category").alias("category_name"))
        .withColumn("source_system", F.lit("online"))
    )
    df_combined = df_offline.unionByName(df_online)
    
    return (
        df_combined.groupBy("category_name")
        .agg(
            F.collect_set("source_system").alias("systems_array")
        )
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
        
        .withColumn("category_src_id", F.md5(F.col("category_name")))
        .withColumn("category_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "category_id",
            "category_name",
            "category_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(
    name = "transportation.silver.BL_3NF_CE_SUB_CATEGORIES",
    comment= "Table for online and offline subcateogory",
    table_properties= {
        "quality": "silver",
        "layer": "silver_3nf"
    }
)
def bl_3nf_ce_subacetorgies():

    df_offline = (
        spark.read.table("transportation.bronze.offline_bronze")
        .select(
            F.col("Sub_Category").alias("sub_category_name"),
            F.col("Category").alias("category_name")
            )
        .withColumn("source_system", F.lit("offline"))
    )
    df_online = (
        spark.read.table("transportation.bronze.online_bronze")
        .select(
            F.col("Sub_Category").alias("sub_category_name"),
            F.col("Category").alias("category_name")
            )
        .withColumn("source_system", F.lit("online"))
    )

    df_bronze_combined = df_offline.unionByName(df_online)

    df_categories_silver = (
        spark.read.table("transportation.silver.BL_3NF_CE_CATEGORIES")
        .select("category_id", "category_name")
    )

    df_unique_sub_cats = (
        df_bronze_combined.groupBy("sub_category_name", "category_name")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
    )

    df_final = df_unique_sub_cats.join(
        df_categories_silver,
        on="category_name", 
        how="left" 
    )

    return (
        df_final
        .withColumn("sub_category_src_id", F.md5(F.col("sub_category_name")))
        .withColumn("sub_category_id", F.monotonically_increasing_id()) 
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "sub_category_id",    
            "category_id",         
            "sub_category_name",
            "sub_category_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(
    name = "transportation.silver.BL_3NF_CE_PRODUCTS",
    comment = "Table for online and offilne products",
    table_properties = {
        "quality": "silver",
        "layer": "silver_3nf"
    }
)
def bl_3nf_ce_products():
    df_offline = (
        spark.read.table("transportation.bronze.offline_bronze")
        .select(F.col('Product_Name').alias("product_name"),
                F.col("Sub_Category").alias("sub_category_name"))
        .withColumn("source_system", F.lit("offline"))
    )

    df_online = (
        spark.read.table("transportation.bronze.online_bronze")
        .select(F.col("Product_Name").alias("product_name"),
                F.col("Sub_Category").alias("sub_category_name"))
        .withColumn("source_system", F.lit('online'))
    )

    df_combined = df_offline.unionByName(df_online)

    df_subcategories = (
        spark.read.table("transportation.silver.bl_3nf_ce_sub_categories")
        .select("sub_category_id", "sub_category_name")
    )

    df_unique_prods = (
        df_combined.groupBy("product_name", "sub_category_name")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
    )

    df_final = df_unique_prods.join(
        df_subcategories,
        on = "sub_category_name",
        how = 'left'
    )   
    return (
        df_final
        .withColumn("product_src_id", F.md5(F.col("product_name")))
        .withColumn("product_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "product_id",
            "sub_category_id",
            "sub_category_name",
            "product_name",
            "product_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )


@dp.table(name="transportation.silver.BL_3NF_CE_COUNTRY")
def bl_3nf_ce_country():
    
    # Odczyt jak poprzednio...
    df_offline = spark.read.table("transportation.bronze.offline_bronze") \
        .select(F.col("store_country").alias("country_name")) \
        .withColumn("source_system", F.lit("offline"))

    df_online = spark.read.table("transportation.bronze.online_bronze") \
        .select(F.col("Country").alias("country_name")) \
        .withColumn("source_system", F.lit("online"))

    # Łączymy dane surowe
    df_combined_raw = df_offline.unionByName(df_online)

    # -------------------------------------------------------------
    # KROK HARMONIZACJI (Czyszczenie przed grupowaniem)
    # -------------------------------------------------------------
    df_cleaned = df_combined_raw.withColumn(
        "country_name",
        # Funkcja F.trim usuwa ewentualne spacje na końcu/początku
        F.when(F.trim(F.col("country_name")) == "USA", F.lit("United States"))
        .when(F.trim(F.col("country_name")) == "UK", F.lit("United Kingdom"))
        # Jeśli nie znaleziono dopasowania, zostaw oryginalną nazwę
        .otherwise(F.trim(F.col("country_name"))) 
    )

    # Teraz wykonujemy naszą standardową logikę na oczyszczonych danych
    return (
        df_cleaned.groupBy("country_name")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
        .withColumn("country_src_id", F.md5(F.col("country_name")))
        .withColumn("country_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "country_id",
            "country_name",
            "country_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(
    name = "transportation.silver.BL_3NF_CE_REGION",
    comment= "Table for online and offline region location",
    table_properties = {
        "quality": "silver",
        "layer": "silver_3nf"
    }
)
def bl_3nf_ce_region():
    
    df_offline = spark.read.table("transportation.bronze.offline_bronze") \
        .select(F.col("store_region").alias("region_name"), F.col("store_country").alias("country_name")) \
        .withColumn("source_system", F.lit("offline"))

    df_online = spark.read.table("transportation.bronze.online_bronze") \
        .select(F.col("Region").alias("region_name"), F.col("Country").alias("country_name")) \
        .withColumn("source_system", F.lit("online"))

    df_combined = df_offline.unionByName(df_online)
    df_cleaned = df_combined.withColumn(
        "country_name",
        F.when(F.trim(F.col("country_name")) == "USA", F.lit("United States"))
        .otherwise(F.trim(F.col("country_name")))
    )

    df_country_silver = spark.read.table("transportation.silver.BL_3NF_CE_COUNTRY").select("country_id", "country_name")

    df_unique_regions = (
        df_cleaned.groupBy("region_name", "country_name")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
    )
    
    df_final = df_unique_regions.join(
        df_country_silver,
        on="country_name",
        how="left"
    )

    return (
        df_final
        .withColumn("region_src_id", F.md5(F.col("region_name")))
        .withColumn("region_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "region_id",
            "country_id", # FK do tabeli Krajów
            "region_name",
            "region_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(
    name = 'transportation.silver.BL_3NF_CE_STATE',
    comment = 'Table for online and offline state location',
    table_properties = {
        "quality": "silver",
        "layer": "silver_3nf"
    }
)
def bl_3nf_ce_state():
    df_offline = (
        spark.read.table("transportation.bronze.offline_bronze")
        .select(F.col("store_state").alias("state_name"),
                F.col("store_region").alias("region_name"))
        .withColumn("source_system", F.lit("offline"))
    )
    df_online = (
        spark.read.table("transportation.bronze.online_bronze")
        .select(F.col("State").alias("state_name"),
                F.col("Region").alias("region_name"))
        .withColumn("source_system", F.lit("online"))
    )

    df_combined = df_offline.unionByName(df_online)

    df_country_silver = (
        spark.read.table("transportation.silver.BL_3NF_CE_REGION")
        .select("region_id", "region_name",)
    )


    df_unique_states = (
        df_combined.groupBy("state_name", "region_name")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
    )
    
    df_final = df_unique_states.join(
        df_country_silver,
        on = "region_name",
        how = "left"
    )

    return (
        df_final
        .withColumn("state_src_id", F.md5(F.col("state_name")))
        .withColumn("state_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "state_id",
            "region_id",
            "state_name",
            "state_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(name="transportation.silver.BL_3NF_CE_CITY")
def bl_3nf_ce_city():
    
    df_offline = spark.read.table("transportation.bronze.offline_bronze") \
        .select(F.col("store_city").alias("city_name"), F.col("store_state").alias("state_name")) \
        .withColumn("source_system", F.lit("offline"))

    df_online = spark.read.table("transportation.bronze.online_bronze") \
        .select(F.col("City").alias("city_name"), F.col("State").alias("state_name")) \
        .withColumn("source_system", F.lit("online"))

    df_combined_raw = df_offline.unionByName(df_online)

    df_cleaned = df_combined_raw.withColumn(
        "city_name", F.trim(F.col("city_name"))
    ).withColumn(
        "state_name", F.trim(F.col("state_name"))
    )

    df_state_silver = spark.read.table("transportation.silver.BL_3NF_CE_STATE").select("state_id", "state_name")

    df_unique_cities = (
        df_cleaned.groupBy("city_name", "state_name")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
    )
    
    df_final = df_unique_cities.join(
        df_state_silver,
        on="state_name",
        how="left"
    )

    return (
        df_final
        .withColumn("city_src_id", F.md5(F.concat(F.col("city_name"), F.col("state_name"))))
        .withColumn("city_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "city_id",
            "state_id",
            "city_name",
            "city_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(name="transportation.silver.BL_3NF_CE_STORE")
def bl_3nf_ce_store():
    
    # 1. Odczyt z Bronze (Zakładamy, że fizyczne sklepy są tylko w offline)
    # UWAGA: Podmień "nazwa_kolumny_sklepu" na faktyczną kolumnę w Twoim pliku CSV!
    df_offline = spark.read.table("transportation.bronze.offline_bronze") \
        .select(
            F.col("store_name"),
            F.col("store_city").alias("city_name"),
            F.col("store_state").alias("state_name"),
            F.col("store_id").alias("store_src_id")
        ) \
        .withColumn("source_system", F.lit("offline"))

    

    df_unique_stores = df_offline.dropDuplicates(["store_src_id", "store_name", "city_name", "state_name"])

    # -------------------------------------------------------------
    # 4. MAGIA 3NF: Słownik Lookup dla Miast
    # Odtwarzamy parę "Miasto + Stan", żeby idealnie wcelować w city_id
    # -------------------------------------------------------------
    df_state = spark.read.table("transportation.silver.BL_3NF_CE_STATE").select("state_id", "state_name")
    df_city = spark.read.table("transportation.silver.BL_3NF_CE_CITY").select("city_id", "state_id", "city_name")
    
    df_city_lookup = df_city.join(df_state, on="state_id", how="inner")

    # 5. Łączymy nasze sklepy z Lookupem po nazwie miasta ORAZ stanu
    df_final = df_unique_stores.join(
        df_city_lookup,
        on=["city_name", "state_name"],
        how="left"
    )

    # 6. Generowanie kluczy i końcowy format
    return (
        df_final
        # Generujemy solidny Hash: Sklep + Miasto
        .withColumn("store_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "store_id",
            "city_id",      # FK: Klucz obcy do tabeli CITY
            "store_name",
            "store_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )




#ujednolic zamieszkanie customera w ingestion bo maja po 1000 roznych co jest malo realistyczne
@dp.table(name="transportation.silver.BL_3NF_CE_CUSTOMER")
def bl_3nf_ce_customer():
    
    # 1. Odczyt TYLKO z systemu Online (bo Offline nie ma klientów)
    # UWAGA: Upewnij się, że nazwy kolumn z Bronze (np. "First_Name") są poprawne
    df_online = spark.read.table("transportation.bronze.online_bronze") \
        .select(
            F.col("customer_name").alias("first_name"), 
            F.col("customer_surname").alias("last_name"),
            F.col("customer_gender").alias("gender"),
            F.col("customer_age").cast("int").alias("age"),
            F.trim(F.col("customer_email")).alias("email"),
            F.col("customer_phone_number").alias("phone_number"),
            F.col("Postal_Code").alias("postal_code"), # Nasz pragmatyczny kod pocztowy
            F.trim(F.col("City")).alias("city_name"),
            F.trim(F.col("State")).alias("state_name")
        ) \
        .withColumn("source_system", F.lit("online"))

    # 2. Czyszczenie: Klient jest unikalny na podstawie adresu e-mail
    # Jeśli ten sam gość kupił 5 razy online, chcemy go w bazie tylko raz!
    df_unique_customers = df_online.dropDuplicates(["email"])

    # -------------------------------------------------------------
    # 3. Lookup dla Miast (dokładnie to samo co przy sklepach)
    # -------------------------------------------------------------
    df_state = spark.read.table("transportation.silver.BL_3NF_CE_STATE").select("state_id", "state_name")
    df_city = spark.read.table("transportation.silver.BL_3NF_CE_CITY").select("city_id", "state_id", "city_name")
    
    df_city_lookup = df_city.join(df_state, on="state_id", how="inner")

    # 4. Przypinamy city_id do naszego klienta
    df_final = df_unique_customers.join(
        df_city_lookup,
        on=["city_name", "state_name"],
        how="left"
    )

    # 5. Generowanie kluczy
    return (
        df_final
        # Używamy e-maila jako bezpiecznego źródła dla hasha
        .withColumn("customer_src_id", F.md5(F.col("email")))
        .withColumn("customer_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "customer_id",
            "city_id",       # FK do tabeli CITY
            "first_name",
            "last_name",
            "gender",
            "age",
            "email",
            "phone_number",
            "postal_code",   # Nasza nowa kolumna
            "customer_src_id",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

@dp.table(name="transportation.silver.BL_3NF_CE_PAYMENT_METHODS")
def bl_3nf_ce_payment_methods():
    
    # 1. Odczyt z systemu Online (czyszczenie ze spacji na start)
    df_online = spark.read.table("transportation.bronze.online_bronze") \
        .select(
            F.col("payment_method"),
            F.col("payment_provider")
        ) \
        .withColumn("source_system", F.lit("online"))

    # 2. Odczyt z systemu Offline (zmieniamy nazwy kolumn na docelowe)
    # UWAGA: Upewnij się, że "store_payment_method" to poprawne nazwy w Twoim pliku
    df_offline = spark.read.table("transportation.bronze.offline_bronze") \
        .select(
            F.col("payment_method"),
            F.col("payment_provider")
        ) \
        .withColumn("source_system", F.lit("offline"))

    # 3. Łączymy dane i odrzucamy ewentualne puste wiersze (śmieci z plików)
    df_combined = df_online.unionByName(df_offline)

    # 4. Nasza sprawdzona logika deduplikacji i łączenia systemów
    df_unique_methods = (
        df_combined.groupBy("payment_method", "payment_provider")
        .agg(F.collect_set("source_system").alias("systems_array"))
        .withColumn(
            "source_system",
            F.when(F.size(F.col("systems_array")) == 2, F.lit("online and offline"))
            .otherwise(F.element_at(F.col("systems_array"), 1))
        )
    )

    # 5. Nadanie kluczy zastępczych i ostateczny Select (zgodny w 100% z diagramem)
    return (
        df_unique_methods
        .withColumn("method_id", F.monotonically_increasing_id())
        .withColumn("insert_dt", F.current_timestamp())
        .withColumn("update_dt", F.current_timestamp())
        .select(
            "method_id",           # PK
            "payment_method",
            "payment_provider",
            "source_system",
            "insert_dt",
            "update_dt"
        )
    )

# Employee SCD2 implementation
dp.create_streaming_table(
    name="transportation.silver.BL_3NF_EMPLOYEE_SCD2",
    schema="""
        employee_src_id STRING,
        first_name STRING,
        last_name STRING,
        gender STRING,
        monthly_salary DECIMAL(10,2),
        store_src_id STRING,
        update_dt TIMESTAMP,
        insert_dt TIMESTAMP,
        __START_AT TIMESTAMP,
        __END_AT TIMESTAMP
    """
)

@dp.temporary_view()
def employee_changes():
    df = spark.readStream.table("transportation.bronze.offline_bronze") \
        .select(
            F.col("employee_id").alias("employee_src_id"),
            F.col("store_id").alias("store_src_id"),
            F.col("employee_name").alias("first_name"), 
            F.col("employee_surname").alias("last_name"),
            F.col("employee_gender").alias("gender"),
            F.col("employee_monthly_salary").cast("decimal(10,2)").alias("monthly_salary"),
            F.current_timestamp().alias("update_dt"),
            F.current_timestamp().alias("insert_dt")
        )
    return df

dp.create_auto_cdc_flow(
    target="transportation.silver.BL_3NF_EMPLOYEE_SCD2",
    source="employee_changes",
    keys=["employee_src_id"],
    sequence_by=F.col("update_dt"),
    except_column_list=["insert_dt"],
    stored_as_scd_type=2
)
    

