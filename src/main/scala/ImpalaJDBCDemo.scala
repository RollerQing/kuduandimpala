import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object ImpalaJDBCDemo {
  def main(args: Array[String]): Unit = {
    val url = "jdbc:impala://node4:21050/default"
    val sql = "select * from bigdata2"

    var conn: Connection = null
    var ps: PreparedStatement = null
    var rs: ResultSet = null

    try{
      conn = DriverManager.getConnection(url)
      ps = conn.prepareStatement(sql)
      rs = ps.executeQuery()

      while (rs.next()){
        val id = rs.getInt("id")
        val name = rs.getString("name")
        val age = rs.getInt("age")

        println(id, name, age)
      }
    }catch{
      case e: Exception => e.printStackTrace()
    }finally {
      rs.close()
      ps.close()
      conn.close()
    }

  }
}
