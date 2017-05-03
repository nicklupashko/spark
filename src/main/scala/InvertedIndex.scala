import org.apache.spark.sql.SparkSession

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("InvertedIndex")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext

    val badStart = Set("xref:", "path:", "from:", "newsgroups:",
      "subject:", "summary:", "keywords:", "message-id:", "date:",
      "expires:", "followup:", "distribution:", "organization:",
      "approved:", "supersedes:", "lines:", "archive-name:", "to:",
      "last-modified:", "version:", "article-i.d.:", "subj:", "cc:",
      "references:", "nntp-posting-host:", "received:", "reply-to:",
      "content-length:", "followup-to:", "sender:")
    val regex = "[a-z]+('[a-z]+)?".r

    sc.wholeTextFiles("d:/test/*")
      .map {
        case (path, text) => path.replaceFirst(".+/(.+/.+)", "$1") ->
          text.toLowerCase.split("\n")
            .filter(line => !badStart.exists(line.startsWith(_)))
            .flatMap(regex.findAllIn(_).toList)
      }
      .flatMap {
        case (path, words) => words.map(word => ((word, path), 1))
      }
      .reduceByKey(_ + _)
      .map { case ((word, path), count) => (word, (count, path)) }
      .reduceByKey((a, b) => (a._1 + b._1, s"${a._2} ${b._2}"))
      .map { case (word, (count, paths)) => s"$word, $count, $paths" }
      .saveAsTextFile("index")

    spark.stop
  }
}