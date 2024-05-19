import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object BroadcastJoin {

  // Create SparkSession
  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  // Get the SparkContext from the SparkSession
  val sparkContext = spark.sparkContext

  // Parallelize a list of Rows
  val rows = sparkContext.parallelize(List(
    Row(0, "zero"),
    Row(1, "first"),
    Row(2, "second"),
    Row(3, "third")
  ))

  val rowsSchema = StructType(Array(
    StructField("id", IntegerType),
    StructField("order", StringType)
  ))

  // small table
  val lookupTable: DataFrame = spark.createDataFrame(rows, rowsSchema)

  // large table
  val table = spark.range(1, 100000000) // column is "id"


  // the classic join
  val joined = table.join(lookupTable, "id")

  // a smarter join
  val joinedSmart = table.join(broadcast(lookupTable), "id")


  val bigTable = spark.range(1, 100000000)
  val smallTable = spark.range(1, 10000) // size estimated by Spark - auto-broadcast
  val joinedNumbers = bigTable.join(smallTable, "id")


  def main(args: Array[String]): Unit = {
//    joined.explain
//    joined.show()

//    joinedSmart.explain
//    joinedSmart.show()

    joinedNumbers.explain
    joinedNumbers.show()
    Thread.sleep(1000000)
  }
}
