import org.apache.spark.sql.functions.first

object Main extends App {

  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val ids = Seq(
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others")).toDF("id", "priority")

  val ids2 = Seq(
    (1, "Others"),
    (1, "MV1"),
    (1, "MV2"),
    (2, "VPV"),
    (2, "Others"),
    (2, "MV1")).toDF("id", "priority")

  val priorities = Seq(
    "MV1",
    "MV2",
    "VPV",
    "Others").zipWithIndex.toDF("name", "rank")

  ids.join(priorities).orderBy($"id", $"rank")
    .where($"priority" === $"name")
    .groupBy($"id")
    .agg(first("priority"))
    .show

  ids2.join(priorities).orderBy($"id", $"rank")
    .where($"priority" === $"name")
    .groupBy($"id")
    .agg(first("priority"))
    .show

}
