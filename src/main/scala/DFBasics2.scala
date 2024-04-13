import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object DFBasics2 {

  val spark = SparkSession.builder()
    .appName("Columns, Expressions, and Functions")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  val df: DataFrame = spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/ebay_watches.csv")


  // select() function to select columns
  import spark.implicits._

  val select1 = df.select("itemLocation","lastUpdated", "sold")
  val select2 = df.select(expr("itemLocation"), expr("lastUpdated"), expr("sold"))
  val select3 = df.select(col("itemLocation"), col("lastUpdated"), col("sold"))
  val select4 = df.select($"itemLocation", $"lastUpdated", $"sold")
  val select5 = df.select(df("itemLocation"), df("lastUpdated"), df("sold"))

  val df2 = df.select(
    col("itemLocation"),
    col("sold"),
    expr("sold * 2 as double_sold"),
    expr("CASE WHEN itemLocation IS NULL THEN 'null_value' ELSE itemLocation END as itemLocation_mod")
  )

  val df3 = df.selectExpr(
    "itemLocation",
    "sold",
    "sold * 2 as double_sold",
    "CASE WHEN itemLocation IS NULL THEN 'null_value' ELSE itemLocation END as itemLocation_mod"
  )


  // create new columns
  val df4 = df
    .select(
      col("sold"),
      (col("sold") * 2).alias("double_sold_v1")
    )
    .withColumn("double_sold_v2", col("sold")*2)

  // case when
  val df5 = df
    .withColumn("seller_new",
      when(col("seller")==="Direct Luxury", lit("Luxury Boutique"))
        .otherwise(col("seller")))

  val df5_2 = df
    .withColumn("seller_new",
      when(col("seller") === "Direct Luxury", lit("Luxury Boutique"))
        .when(col("seller") === "WATCHGOOROO", lit("Watch Gooroo"))
        .when(col("seller") === "ioomobile", lit("Ioo Mobile"))
        .otherwise(col("seller")))

  // rename
  val df6 = df.withColumnRenamed("itemLocation", "item_location")
  val df6_2 = df.select(col("itemLocation").alias("item_location"))

  // drop
  val df7 = df.drop("availableText", "item_location")

  // filter
  val df8 = df.filter(col("seller")==="Direct Luxury")
  val df8_2 = df.where(col("seller")=!="Direct Luxury")
  val df8_3 = df.filter("seller = 'Direct Luxury'")
  val df8_4 = df.filter(col("seller").isin("Direct Luxury", "Sigmatime", "ioomobile"))
  val df8_5 = df.filter(not(col("seller").isin("Direct Luxury", "Sigmatime", "ioomobile")))
  val dfWithNull = df.filter(col("itemLocation").isNull)
  val dfWithoutNull = df.filter(col("itemLocation").isNotNull)

  // chain filters
  val df9 = df.filter(col("seller")==="Direct Luxury").filter(col("sold")>1)
  val df9_2 = df.filter((col("seller")==="Direct Luxury") || (col("sold")>1))
  val df9_3 = df.filter((col("seller") === "Direct Luxury").and(col("sold") > 1))
  val df9_4 = df.filter("seller = 'Direct Luxury' and sold > 1")

  // order by
  val orderBy = df
    .orderBy(asc_nulls_last("sold"), col("title"))
  val orderBy_desc = df
    .orderBy(desc_nulls_last("sold"), desc("title"))

  // summary statistics
  val df_summary = df.select(min("sold"), max("sold"), avg("sold"))
  val df_summary_casting = df.select(min(col("sold").cast("int")), max(col("sold").cast("int")), avg(col("sold").cast("int")))

  //  Union
  val df_sales: DataFrame = spark.read
    .option("header", "true")
    .option("delimiter", ",")
    .csv("src/main/resources/data/Walmart_sales.csv")

  val df10 = df.select("availableText").union(df_sales.select("Weekly_Sales"))
  val df10_2 = df.unionByName(df_sales, allowMissingColumns = true)


  // convert to date and timestamp
  val df11 = df
    .withColumn("lastUpdated_timestamp", to_timestamp(col("lastUpdated"), "MMM dd, yyyy HH:mm:ss z"))
    .withColumn("lastUpdated_date", to_date(col("lastUpdated"), "MMM dd, yyyy HH:mm:ss z"))


  // convert date or timestamp to string
  val orders = Seq(
    Row("sandwich", "big", 10, "2024-03-24", "2024-03-24:14:31:20"),
    Row("pizza", "small", 15, "2024-03-22", "2024-03-22:21:00:12")
  )
  val schema = StructType(Seq(
    StructField("food", StringType, nullable = true),
    StructField("size", StringType, nullable = true),
    StructField("cost", IntegerType, nullable = true),
    StructField("order_date", StringType, nullable = true),
    StructField("order_timestamp", StringType, nullable = true)
  ))
  val df_orders = spark.createDataFrame(spark.sparkContext.parallelize(orders), schema)
  val df_orders_clean = df_orders
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("order_timestamp", to_timestamp(col("order_timestamp"), "yyyy-MM-dd:HH:mm:ss"))
  val df_date_formatted =
    df_orders_clean
      .withColumn("date_formatted", date_format(col("order_date"), "ddMMMyyyy"))
      .withColumn("timestamp_formatted", date_format(col("order_timestamp"), "ddMMMyyyy HH:mm:ss"))

  // extract substring
  val df12 = df
    .withColumn("itemNumber_v2", substring(col("itemNumber"), 0, 5))


  // remove blank spaces
  import spark.implicits._
  val orders_2 = Seq(
    (" sandwich  ", "big", 10, "2024-03-24"),
    ("   pizza", "small", 15, "2024-03-22"),
    ("salad  ", "small", 15, "2024-03-22")
  )
  val df13 = orders_2.toDF("food", "size", "cost", "order_date")

  val df13_2 = df13
    .withColumn("trim_food", trim(col("food")))
    .withColumn("ltrim_food", ltrim(col("food")))
    .withColumn("rtrim_food", rtrim(col("food")))

  // pad strings
  val df17 = df.withColumn("lpad_sold", lpad(col("sold"), 4, "0"))

  // concatenate strings
  val tmp = Seq(
    ("sandwich", "big"),
    ("pizza", null),
    (null, "small"),
    (null, null)
  )
  val df18 = tmp.toDF("food", "size")
  val df18_2 = df18
    .withColumn("concat", concat(col("food"), lit("_"), col("size")))
    .withColumn("concat_ws", concat_ws("_", col("food"), col("size")))

  // regexp_replace
  val df19 = df
    .withColumn("dollars_to_euros", regexp_replace(col("priceWithCurrency"), "US \\$", "EUR €"))
  val df18_final = df18_2
    .withColumn("concat_ws_final", when(col("concat_ws")==="", lit(null)).otherwise(col("concat_ws")))

  // split
  val df20 = df
    .withColumn("itemLocation_splitted", split(col("itemLocation"), ","))
    .withColumn("first_element", split(col("itemLocation"), ",")(0))
    .withColumn("second_element", split(col("itemLocation"), ",")(1))
    .withColumn("second_element_fix", trim(split(col("itemLocation"), ",")(1)))
    .withColumn("concat_elements", concat_ws("_", col("first_element"), col("second_element")))
    .withColumn("concat_elements_fix", concat_ws("_", col("first_element"), col("second_element_fix")))


  // aggregation function
  val df21 = df
    .groupBy("type").count()
  val df21_2 = df
    .withColumn("price_fixed", regexp_replace(regexp_replace(col("priceWithCurrency"), "US \\$", ""), "\\,", "").cast("float"))
    .groupBy("type").agg(count("*"), min("price_fixed"), max("price_fixed"), avg("price_fixed"))
  val df21_3 = df
    .withColumn("price_fixed", regexp_replace(regexp_replace(col("priceWithCurrency"), "US \\$", ""), "\\,", "").cast("float"))
    .groupBy("seller", "type").agg(count("*"), min("price_fixed"), max("price_fixed"), avg("price_fixed"))


  // window function
  val windowSpec = Window.partitionBy("seller").orderBy(desc("lastUpdated_timestamp"))
  val df22 = df
    .withColumn("lastUpdated_timestamp", to_timestamp(col("lastUpdated"), "MMM dd, yyyy HH:mm:ss z"))
    .withColumn("row_number", row_number().over(windowSpec))
    .withColumn("rank", rank().over(windowSpec))
    .withColumn("dense_rank", dense_rank().over(windowSpec))
    .orderBy(col("seller"), desc("lastUpdated_timestamp"))


  // remove duplicates
  val df_clean = df
    .withColumn("lastUpdated_timestamp", to_timestamp(col("lastUpdated"), "MMM dd, yyyy HH:mm:ss z"))

  val df23 = df_clean.distinct()
  val df23_2 = df_clean.select("type").distinct()

  val df24 = df_clean.dropDuplicates()
  val df24_2 = df_clean.dropDuplicates("itemLocation", "lastUpdated_timestamp")

  val windowSpec2 = Window.partitionBy("itemLocation", "lastUpdated_timestamp").orderBy("lastUpdated_timestamp")
  val df_24_2_fix = df_clean
    .withColumn("row_number", row_number().over(windowSpec2))
    .filter(col("row_number")===1)


  def main(args: Array[String]): Unit = {
    // select
//    df2.show(5)
//    df3.show(5)
//    df4.show(5)

    //when
//    df5.select("seller", "seller_new").show(5)
//    df5_2.select("seller", "seller_new").show(5)

    // renaming
//    df6.show(5)
//    df6_2.show(5)

    // drop
//    df7.show()

    // filter
//    df8.show(5)
//    df8_2.show(5)
//    df8_3.show(5)
//    df9.show()
//    df9_2.show()
//    df9_3.show()
//    df9_4.show()

    // order
//    orderBy.show(5)

    //summary statistic
//    df_summary.show()

    // union
//    df10.show()
//    df10_2.show()

    // convert to timestamp
//    df11.show()

    // date_format
//    df_orders_clean
//    df_date_formatted

    // substring
//    df12.show()

    // trim
//    df13_2.show()

//    df14.select("lastUpdated", "lastUpdated_timestamp", "lastUpdated_date").printSchema()
//    df14.select("lastUpdated", "lastUpdated_timestamp", "lastUpdated_date").show(5, truncate = false)
//    df_date_formatted.show()
//    df15.select("itemNumber", "itemNumber_v2").show(5)
//    df16_2.select("food", "trim_food", "ltrim_food", "rtrim_food").show()
//    df17.select("sold", "lpad_sold").show(5)
//    df18_2.show()
//    df19.select("priceWithCurrency", "dollars_to_euros").show(5)
//    df18_final.show(5)
//    df20.select("itemLocation", "itemLocation_splitted", "first_element", "second_element", "concat_elements", "concat_elements_fix").show(5)
//    df21.show()
//    df21_2.show()
//    df21_3.show(5)
//    df22.select("seller", "lastUpdated_timestamp", "row_number", "rank", "dense_rank").show()
//    df_24_2_fix.show()

  }
}
