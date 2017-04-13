import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}

object TriangleCounting {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TriangleCounting")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext

    val count =
      GraphLoader.edgeListFile(sc, "d:/graph.txt", false)
        .partitionBy(PartitionStrategy.RandomVertexCut)
        .triangleCount.vertices.values.sum.toLong / 3

    println(count)

    spark.stop
  }
}