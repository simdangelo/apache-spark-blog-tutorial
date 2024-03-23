import org.apache.spark.sql.SparkSession

object firstApplication {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("First Spark Application")
    .getOrCreate()

  def printDF(): Unit = {
    val users_data = Seq(
      ("Paolo", "Rossi", 54, "Italia"),
      ("John", "Doe", 24, "USA"),
    )

    val usersDF = spark.createDataFrame(users_data)

    usersDF.show()
  }


  def main(args: Array[String]): Unit = {
    printDF()
  }
}
