

object DataSkew {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.types.IntegerType

  val spark = SparkSession.builder
    .appName("Data Skew Example")
    .master("local[4]")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) // -1 to deactivate auto-broadcast


  val transactions_file = "src/main/resources/data/data_skew/transactions.parquet"
  val customer_file = "src/main/resources/data/data_skew/customers.parquet"

  val df_transactions = spark.read.parquet(transactions_file) // skewed dataframe
  val df_customers = spark.read.parquet(customer_file)

  val df_transactions_grouped = df_transactions
    .groupBy("cust_id")
    .agg(countDistinct("txn_id").alias("ct"))
    .orderBy(desc("ct"))

  // classic join
  val df_txn_details = df_transactions.join(df_customers, "cust_id", "inner")


  // SOLUTION 1: enable AQE
  def join_aqe() = {
    spark.conf.set("spark.sql.adaptive.enabled", "true") // activate AQE
    spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true") // activate AQE

    val df_txn_details_aqe = df_transactions.join(df_customers, "cust_id", "inner")
    df_txn_details_aqe
  }
  val df_txn_details_aqe = join_aqe()


  // SOLUTION 2: broadcast join
  def join_broadcast() = {
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", "true")

    val df_txn_details_broadcasted = df_transactions.join(broadcast(df_customers), "cust_id", "inner")
    df_txn_details_broadcasted
  }
  val df_txn_details_broadcasted = join_broadcast()


//  SOLUTION 3: Salting Technique
  def join_salting(): Unit = {
    spark.conf.set("spark.sql.shuffle.partitions", "3")
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    // Create df_uniform DataFrame
    val df_uniform = spark.createDataFrame((0 until 1000000).map(Tuple1(_))).toDF("value")

    // Add partition column and show partition counts
    val df_uniform_show = df_uniform
      .withColumn("partition", spark_partition_id())
      .groupBy("partition")
      .count()
      .orderBy("partition")
//    df_uniform_show.show(15, false)

    // Create skewed DataFrames
    val df0 = spark.createDataFrame(Seq.fill(999990)(0).map(Tuple1(_))).toDF("value").repartition(1)
    val df1 = spark.createDataFrame(Seq.fill(15)(1).map(Tuple1(_))).toDF("value").repartition(1)
    val df2 = spark.createDataFrame(Seq.fill(10)(2).map(Tuple1(_))).toDF("value").repartition(1)
    val df3 = spark.createDataFrame(Seq.fill(5)(3).map(Tuple1(_))).toDF("value").repartition(1)

    // Union skewed DataFrames
    val df_skew = df0.union(df1).union(df2).union(df3)
    //    df_skew.show(5, false)

    // Add partition column and show partition counts
    val df_skew_show = df_skew
      .withColumn("partition", spark_partition_id())
      .groupBy("partition")
      .count()
      .orderBy("partition")

    print("Data distribution across Partitions before Salting Technique:\n")
    df_skew_show.show()


    val SALT_NUMBER = spark.conf.get("spark.sql.shuffle.partitions").toInt

    // Add salt column to df_skew
    val df_skew_with_salt = df_skew.withColumn("salt", (rand() * SALT_NUMBER).cast(IntegerType))
//    df_skew_with_salt.show()

    //     Add salt_values and explode to create salt column in df_uniform
    val saltValues = (0 until SALT_NUMBER).map(lit(_)).toArray
    val df_uniform_with_salt = df_uniform
      .withColumn("salt_values", array(saltValues: _*))
      .withColumn("salt", explode(col("salt_values")))

    // Perform the join operation between df_skew_with_salt and df_uniform_with_salt
    val df_joined = df_skew_with_salt.join(df_uniform_with_salt, Seq("value", "salt"), "inner")

    // Add partition column and show partition counts
    val df_joined_show = df_joined
      .withColumn("partition", spark_partition_id())
      .groupBy("partition")
      .count()
      .orderBy("partition")

    print("Data distribution across Partitions after Salting Technique:\n")
    df_joined_show.show()
  }



  def main(args: Array[String]): Unit = {
//    df_transactions_grouped.show(5, truncate=false) // show the skewness
//    df_txn_details.count() // classic join
//    df_txn_details_aqe.count() // AQE enabled
//    df_txn_details_broadcasted.count() // Broadcast join
    join_salting()
    Thread.sleep(1000000)

  }
}
