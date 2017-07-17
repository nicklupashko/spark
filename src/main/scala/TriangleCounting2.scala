import org.apache.spark.sql.SparkSession

object TriangleCounting2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TriangleCounting")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext

    val graph = sc.textFile("graph.txt").flatMap(s => {
      val a = s.split(" ").map(_.toInt).sorted
      if (a(0) == a(1)) List()
      else List((a(0), a(1)))
    }).distinct.cache
	
    println(graph.join(graph).keyBy(_._2).join(graph.map(_ -> 1)).count)

    spark.stop
  }
}