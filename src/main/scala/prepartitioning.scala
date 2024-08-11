import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object prepartitioning {

  val spark = SparkSession.builder
    .appName("Pre-Partitioning")
    .master("local[*]")
    .config("spark.sql.adaptive.enabled", "false")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  val initialTable = spark.range(1, 10000000).repartition(10)
  val narrowTable = spark.range(1, 5000000).repartition(7)

  def addColumns[T](df: Dataset[T], n: Int): DataFrame = {
    val newColumns = (1 to n).map(i => s"id * $i as newCol$i")
    df.selectExpr("id" +: newColumns: _*)
  }


  // SCENARIO 1
  val wideTable = addColumns(initialTable, 30)
  val join1 = wideTable.join(narrowTable, "id")

  // SCENARIO 2
  val altNarrow = narrowTable.repartition($"id")
  val altInitial = initialTable.repartition($"id")
  val join2 = altInitial.join(altNarrow, "id")
  val result2 = addColumns(join2, 30)

  // SCENARIO 3
  val enhanceColumnsFirst = addColumns(initialTable, 30)
  val repartitionedNarrow = narrowTable.repartition($"id")
  val repartitionedEnhance = enhanceColumnsFirst.repartition($"id") // <- it's USELESS!!
  val result3 = repartitionedEnhance.join(repartitionedNarrow, "id")
  val result3_bis = enhanceColumnsFirst.join(repartitionedNarrow, "id")



  def main(args: Array[String]): Unit = {
    join1.show()
    result2.show()
    result3.show()
    result3_bis.show()

    join1.explain
    result2.explain
    result3.explain
    result3_bis.explain

    Thread.sleep(1000000000)
  }
}
