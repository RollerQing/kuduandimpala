import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

// 核心：KuduContext
object KuduWithSparkDemo {
  def main(args: Array[String]): Unit = {
    val kuduMaster = "node4:7051"
    val tableName = "bigdata2"
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getCanonicalName)
    val spark = SparkSession.builder().config(conf).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val kuduContext = new KuduContext(kuduMaster, spark.sparkContext)

//    createTable(kuduContext, spark.sparkContext, tableName)
//    insertTable(kuduContext, spark, tableName)
//    deleteTable(kuduContext, spark, tableName)
//    queryTable(kuduContext, spark, tableName)
    upserttable(kuduContext, spark, tableName)
    queryTable(kuduContext, spark, tableName)

    spark.close()
  }

  def createTable(kudu: KuduContext, spark: SparkContext, tname: String) = {
    val schema = (new StructType)
      .add("id", IntegerType)
      .add("name", StringType)
      .add("age", IntegerType)

    val keys = List("id")
    import scala.collection.JavaConversions._
    val options = new CreateTableOptions().addHashPartitions(List("id"), 5)
      .setNumReplicas(1)

    if (!kudu.tableExists(tname))
      kudu.createTable(tname, schema, keys, options)
  }

  def insertTable(kudu: KuduContext, spark: SparkSession, tname: String) = {
    val random = scala.util.Random
    val lst = for (i <- 1 to 300) yield {
      (i, s"username$i", random.nextInt(60))
    }
    val df = spark.createDataFrame(lst).toDF("id", "name", "age")
    kudu.insertRows(df, tname)
  }

  def queryTable(kudu: KuduContext, spark: SparkSession, tname: String) = {
    val rdd: RDD[Row] = kudu.kuduRDD(spark.sparkContext, tname, List("id", "name", "age"))
    rdd.foreach(println)
  }

  def deleteTable(kudu: KuduContext, spark: SparkSession, tname: String) = {
    val lst = (100 to 300).toList.zipWithIndex
    val df = spark.createDataFrame(lst).toDF("id", "idx")
      .select("id")
    kudu.deleteRows(df, tname)
  }

  def upserttable(kudu: KuduContext, spark: SparkSession, tname: String) = {
    val lst = List((95, "username95", 100),
      (96, "username95", 100),
      (97, "username95", 100),
      (98, "username95", 100),
      (99, "username95", 100),
      (100, "username95", 100),
      (101, "username95", 100),
      (102, "username95", 100),
      (103, "username95", 100),
      (104, "username95", 100),
      (105, "username95", 100)
    )
    val df = spark.createDataFrame(lst).toDF("id", "name", "age")
    kudu.upsertRows(df, tname)
  }
}
