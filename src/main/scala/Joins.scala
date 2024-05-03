import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Joins {

  val spark = SparkSession.builder()
    .appName("Spark Joins")
    .config("spark.master", "local[*]")
    .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")


  import spark.implicits._

  val bands = Seq(
    (1, "AC/DC", "Sydney", 1973),
    (0, "Led Zeppelin", "London", 1968),
    (3, "Metallica", "Los Angeles", 1981),
    (4, "The Beatles", "Liverpool", 1960)
  )
  val bandsDF = bands.toDF("id", "name", "hometown", "year")

  val guitars = Seq(
    (0, "EDS-1275", "Gibson", "Electric double-necked"),
    (5, "Stratocaster", "Fender", "Electric"),
    (1, "SG", "Gibson", "Electric"),
    (2, "914", "Taylor", "Acoustic"),
    (3, "M-II", "ESP", "Electric"),
  )
  val guitarsDF = guitars.toDF("id", "model", "make", "guitarType")

  val guitarists = Seq(
    (0, "Jimmy Page", Seq(0), 0),
    (1, "Angus Young", Seq(1), 1),
    (2, "Eric Clapton", Seq(1, 5), 2),
    (3, "Kirk Hammett", Seq(3), 3)
  )
  val guitaristsDF = guitarists.toDF("id", "name", "guitars", "band")


  // condition
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")

  // left join (== left_outer)
  val guitaristsBandsDF_left = guitaristsDF.join(bandsDF, joinCondition, "left")
  // inner join
  val guitaristsBandsDF_inner = guitaristsDF.join(bandsDF, joinCondition, "inner")
  // right join (== right_outer)
  val guitaristsBandsDF_right = guitaristsDF.join(bandsDF, joinCondition, "right")

  // full outer: everything in the inner join + all the rows in the BOTH table, with nulls in there the data is missing
  val guitaristsBandsDF_full_outer = guitaristsDF.join(bandsDF, joinCondition, "full_outer")

  // left_semi
  val guitaristsBandsDF_left_semi = guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // left_anti
  val guitaristsBandsDF_left_anti = guitaristsDF.join(bandsDF, joinCondition, "left_anti")


  // solutions to problems
  val guitaristsBandsDF_left_v2 = guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band", "left")

  val guitaristsBandsDF_left_v3 = guitaristsDF.join(bandsDF, joinCondition, "left").drop(bandsDF.col("id"))

  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  val guitaristsBandsDF_left_v4 = guitaristsDF.join(bandsModDF, guitaristsDF.col("band")===bandsModDF.col("bandId"), "left")


  def main(args: Array[String]): Unit = {
    guitaristsBandsDF_left.show()
    guitaristsBandsDF_inner.show()
    guitaristsBandsDF_right.show()
    guitaristsBandsDF_full_outer.show()
    guitaristsBandsDF_left_semi.show()
    guitaristsBandsDF_left_anti.show()

    guitaristsBandsDF_left_v2.select("id", "band").show()
    guitaristsBandsDF_left_v3.select("id", "band").show()
    guitaristsBandsDF_left_v4.select("id", "band").show()
  }
}
