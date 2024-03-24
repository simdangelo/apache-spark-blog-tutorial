import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

object DFBasics {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Basic operations with DataFrame")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  //READ DF

  // Manual DF
  def manualDF_v1(): Unit = {
    val orders = Seq(
      ("sandwich", "big", 10, "2024-03-24"),
      ("pizza", "small", 15, "2024-03-22")
    )
    import spark.implicits._

    val df = orders.toDF("food", "size", "cost", "order_date")

    df.printSchema()
    df.show()
  }

  def manualDF_v2(): Unit = {
    val orders = Seq(
      Row("sandwich", "big", 10, "2024-03-24"),
      Row("pizza", "small", 15, "2024-03-22")
    )
    val schema = StructType(Seq(
      StructField("food", StringType, nullable = true),
      StructField("size", StringType, nullable = true),
      StructField("cost", IntegerType, nullable = true),
      StructField("order_date", StringType, nullable = true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(orders), schema)

    df.printSchema()
    df.show()
  }

  // CSV
  def readCsv_v1(): Unit = {
    val df = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .csv("src/main/resources/data/Walmart_sales.csv")

    df.printSchema()
    df.show(5)
  }

  def readCsv_v2(): Unit = {
    val csvSchema = StructType(
      Array(
        StructField("Store", StringType),
        StructField("Date", DateType),
        StructField("Weekly_Sales", DoubleType),
        StructField("Holiday_Flag", IntegerType),
        StructField("Temperature", DoubleType),
        StructField("Fuel_Price", DoubleType),
        StructField("CPI", DoubleType),
        StructField("Unemployment", DoubleType)
      )
    )

    val df = spark.read
      .schema(csvSchema)
      .option("header", "true")
      .option("dateFormat", "dd-MM-yyyy")
      .csv("src/main/resources/data/Walmart_sales.csv")

    df.printSchema()
    df.show(5)
  }

  // JSON
  def readJson(): Unit = {
    val jsonSchema = StructType(
      Array(
        StructField("sepalLength", DoubleType),
        StructField("sepalWidth", DoubleType),
        StructField("petalLength", DoubleType),
        StructField("petalWidth", DoubleType),
        StructField("species", StringType),
      )
    )

    val df = spark.read
      .schema(jsonSchema)
      .json("src/main/resources/data/iris.json")

    df.printSchema()
    df.show(5)
  }

  // PARQUET
  def readParquet(): Unit = {
    val df = spark.read
      .parquet("src/main/resources/data/MT cars.parquet")

    df.printSchema()
    df.show(5)
  }

  // Remote DB
  def readPostgres(): Unit = {
    val df = spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url", "jdbc:postgresql://localhost:5432/my_database")
      .option("user", "docker")
      .option("password", "docker")
      .option("dbtable", "my_table")
      .load()

    df.printSchema()
    df.show()
  }

  //  ---------------------------------------------------------

  // WRITE DF
  def readCsv_file(): DataFrame  = {
    val csvSchema = StructType(
      Array(
        StructField("Store", StringType),
        StructField("Date", DateType),
        StructField("Weekly_Sales", DoubleType),
        StructField("Holiday_Flag", IntegerType),
        StructField("Temperature", DoubleType),
        StructField("Fuel_Price", DoubleType),
        StructField("CPI", DoubleType),
        StructField("Unemployment", DoubleType)
      )
    )
    val df = spark.read
      .schema(csvSchema)
      .option("header", "true")
      .option("dateFormat", "dd-MM-yyyy")
      .csv("src/main/resources/data/Walmart_sales.csv")

    df
  }

  def writeParquet(): Unit = {
    readCsv_file().write
      .mode("overwrite") // or (SaveMode.Overwrite)
      .parquet("src/main/resources/data/output/walmart_sales.parquet")
  }

  def writeJson(): Unit = {
    readCsv_file().write
      .mode("overwrite") // or (SaveMode.Overwrite)
      .json("src/main/resources/data/output/walmart_sales.json")
  }

  def main(args: Array[String]): Unit = {
    // Reading
//    manualDF_v1()
//    manualDF_v2()
//    readCsv_v1()
//    readCsv_v2()
//    readJson()
//    readParquet()
    readPostgres()

    // Writing
//    writeParquet()
//    writeJson()
  }
}
