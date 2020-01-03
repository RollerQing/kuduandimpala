import org.apache.kudu.ColumnSchema.ColumnSchemaBuilder
import org.apache.kudu.{Schema, Type}
import org.apache.kudu.client._
import org.apache.kudu.client.KuduClient.KuduClientBuilder

object KuduWithJavaAPI {
  def main(args: Array[String]): Unit = {
    // 创建连接
    val kuduMaster = "192.168.40.64:7051"
    val kuduClient: KuduClient = new KuduClientBuilder(kuduMaster).build()
    val tableName = "bigdata1"

    // 创建表
    createTable(kuduClient, tableName)

    // 删除表
//    dropTable(kuduClient, tableName)

    // 插入数据
//    insertTable(kuduClient, tableName)

    // 数据查询
//    queryTable(kuduClient, tableName)

    // 更新数据 （update）
//    updateTable(kuduClient, tableName)

    // 删除数据（delete）
    deleteTable(kuduClient, tableName)

    queryTable(kuduClient, tableName)


    // 释放连接
    kuduClient.close()
  }

  // 创建表
  def createTable(kudu: KuduClient, tname: String) = {
    import scala.collection.JavaConversions._
    val schema = new Schema(List(
      new ColumnSchemaBuilder("id", Type.INT32).key(true).build(),
      new ColumnSchemaBuilder("name", Type.STRING).nullable(false).build(),
      new ColumnSchemaBuilder("age", Type.INT32).build()
    ))
    val options = new CreateTableOptions().addHashPartitions(List("id"), 5)
      .setNumReplicas(1)

    if (!kudu.tableExists(tname))
      kudu.createTable(tname, schema, options)
  }

  // 删除表
  def dropTable(kudu: KuduClient, tname: String) = {
    if (kudu.tableExists(tname))
      kudu.deleteTable(tname)
  }

  // 插入数据
  def insertTable(kudu: KuduClient, tname: String) = {
    // 1 新建session
    val session: KuduSession = kudu.newSession()

    // 2 KuduTable对象
    val kuduTable: KuduTable = kudu.openTable(tname)

    val random = scala.util.Random
    for (i <- 1 to 100) {
      // 3 Insert 对象
      val insert: Insert = kuduTable.newInsert()

      // 4 Row 对象
      val row: PartialRow = insert.getRow

      // 5 向Row中添加数据
      row.addInt("id", i)
      row.addString("name", s"username$i")
      row.addInt("age", random.nextInt(70))

      // 6 执行insert操作
      session.apply(insert)
    }
    // 资源释放
    session.close()
  }

  // 数据查询
  def queryTable(kudu: KuduClient, tname: String) = {
    // 获取KuduTable
    val table: KuduTable = kudu.openTable(tname)

    // 创建Scanner（指定条件 => 谓词、投影）
    import scala.collection.JavaConversions._
    val predicateAge: KuduPredicate = KuduPredicate.newComparisonPredicate(new ColumnSchemaBuilder("age", Type.INT32).build(),
      KuduPredicate.ComparisonOp.GREATER, 50)
    val scanner: KuduScanner = kudu.newScannerBuilder(table)
//        .addPredicate(predicateAge)
      .setProjectedColumnNames(List("id", "name", "age"))
      .build()

    // 获取结果
    while (scanner.hasMoreRows) {
      val iterator: RowResultIterator = scanner.nextRows()
      while (iterator.hasNext) {
        val row: RowResult = iterator.next()
        val id = row.getInt("id")
        val name = row.getString("name")
        val age = row.getInt("age")
        println(id, name, age)
      }
    }

    // 关闭资源
    scanner.close()
  }


  def updateTable(kudu: KuduClient, tname: String) = {
    val session: KuduSession = kudu.newSession()
    val kuduTable: KuduTable = kudu.openTable(tname)
    val update: Update = kuduTable.newUpdate()
    val row: PartialRow = update.getRow
    row.addInt("id", 50)
    row.addString("name", "name50")
    row.addInt("age", 100)
    session.apply(update)
    session.close()
  }

  // delete操作，在主键上进行
  def deleteTable(kudu: KuduClient, tname: String) = {
    val session: KuduSession = kudu.newSession()
    val kuduTable: KuduTable = kudu.openTable(tname)
    for (i <- 50 to 100) {
      val delete = kuduTable.newDelete()
      val row: PartialRow = delete.getRow
      row.addInt("id", i)
      session.apply(delete)
    }
    session.close()
  }


}


//任务：
//1. 创建表(Hash分区 create) / 删除表（drop table）
//2. 插入数据（insert） / 查询数据（select）
//3. 更新数据 （update）/ 删除数据（delete）
//4. Range分区 / Multilevel分区
//  备注：
//1. 核心类KuduClient
//2. 使用Java API，用scala完成编码

// 没有SQL接口