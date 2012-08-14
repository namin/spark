package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

object LinkReverseSite {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: LinkReverseSite <host> <segment> <urlFilter>")
      System.exit(1)
    }

    val segment = if (args.length > 1) args(1) else "1341690147253"
    val reUrlFilter: String => Boolean = if (args.length > 2) {
      val urlFilter = args(2).r
      (x => urlFilter.findFirstIn(x) != None)
    } else (x => true)

    val sc = new SparkContext(args(0), "Generate Link Reverse Site", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val s = sc.textFile("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@namin-rev/" + segment)

    for (line <- s) {
      val urls = line.split(" ")
      if (!urls.isEmpty && reUrlFilter(urls(0))) { // todo: urls shouldn't be empty if data prune correctly upstream
        val uid = SiteUtils.getUid(urls(0))
        SiteUtils.s3save("namin-live", "linkrev/" + segment + "/" + uid + ".txt", line)
      }
    }

    System.exit(0)
  }
}
