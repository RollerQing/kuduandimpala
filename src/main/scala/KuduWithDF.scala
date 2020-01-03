import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.kudu.spark.kudu.{KuduDataFrameReader, KuduDataFrameWriter}

object KuduWithDF {
  def main(args: Array[String]): Unit = {
    val kuduMaster = "node4:7051"
    val tableName = "bigdata2"
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = spark.read
      .option("kudu.master", kuduMaster)
      .option("kudu.table", tableName)
      .kudu
    df.show()

    import spark.implicits._
    df.select($"id" + 1000 as "id", $"name", $"age")
      .write
      .mode("append")
      .option("kudu.master", kuduMaster)
      .option("kudu.table", tableName)
      .kudu

    spark.close()
  }
}
