import org.apache.spark.sql.SparkSession

object InvertedIndex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("InvertedIndex")
      .master("local[*]")
      .getOrCreate
    val sc = spark.sparkContext

    val badStart = Set("Xref", "Path", "From", "Newsgroups",
      "Subject", "Summary", "Keywords", "Message-ID", "Date",
      "Expires", "Followup", "Distribution", "Organization",
      "Approved", "Supersedes", "Lines", "Archive-name", "To",
      "Last-modified", "Version", "Article-I.D.", "Subj", "CC",
      "References", "Nntp-Posting-Host", "Received", "reply-to",
      "content-length", "followup-to", "sender")
      .map(_.toLowerCase + ":")
    val regex = """[a-z][a-z']+""".r

    sc.wholeTextFiles("d:/test/*")
      .map {
        case (path, text) => path.replaceFirst(".+/", "") ->
          text.toLowerCase.split("\n")
            .filter(l => badStart.forall(!l.startsWith(_)))
            .flatMap(regex.findAllIn(_).toList)
      }
      .flatMap {
        case (path, words) => words.map(w => ((w, path), 1))
      }
      .reduceByKey(_ + _)
      .map { case ((word, path), count) => (word, (count, path)) }
      .reduceByKey((l, r) => (l._1 + r._1, s"${l._2} ${r._2}"))
      .map { case (word, (count, path)) => s"$word, $count, $path" }
      .saveAsTextFile("index")

    spark.stop
  }
}