import org.apache.spark.sql.SparkSession

import scala.io.Source

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("InvertedIndex")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext

    /**
      * Created for `20 Newsgroup DataSet` from
      * http://www.cs.cmu.edu/afs/cs/project/theo-20/www/data/news20.html
      */
    val ignore = Source.fromFile("ignore.txt").getLines().toSet
    val regex = "[a-z]+('[a-z]+)?".r
    val path = "path/to/20_newsgroup/*"

    sc.wholeTextFiles(path)
      .map {
        case (path, text) => path.replaceFirst(".+/(.+/.+)", "$1") ->
          text.toLowerCase.split("\n")
            .filter(line => !ignore.exists(line.startsWith(_)))
            .flatMap(regex.findAllIn(_).toList)
      }
      .flatMap {
        case (path, words) => words.map(word => ((word, path), 1))
      }
      .reduceByKey(_ + _)
      .map {
        case ((word, path), count) => (word, (count, path))
      }
      .reduceByKey {
        case ((leftCount, leftPath), (rightCount, rightPath)) =>
          (leftCount + rightCount, s"$leftPath $rightPath")
      }
      .map {
        case (word, (count, paths)) => s"$word, $count, $paths"
      }
      .saveAsTextFile("index")

    spark.stop
  }
}