import org.apache.spark.sql.SparkSession

object test {

  System.setProperty("HADOOP_USER_NAME", "heluo")

  val spark: SparkSession = SparkSession.builder()
    .master("local[*]")
    .appName("medical")
    .config("spark.sql.parquet.writeLegacyFormat", "true")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    spark.read.table("ods.consultation_inc").show(false)
    spark.read.table("dwd.trade_consultation_inc").show(false)

    spark.stop()
  }
}
