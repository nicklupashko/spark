import org.apache.spark.sql.SparkSession

object TriangleCounting2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TriangleCounting2")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext

    val graph = sc.textFile("graph.txt").flatMap(edge => {
      val vertices = edge.split(" ").map(_.toInt).sorted
      if (vertices(0) == vertices(1)) List()
      else List((vertices(0), vertices(1)))
    }).distinct.cache

    println(graph.join(graph).keyBy(_._2).join(graph.map(_ -> 1)).count)

    spark.stop
  }
}