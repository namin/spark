package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

object LinkReverseSite {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: LinkReverseSite <host> <segment>")
      System.exit(1)
    }

    val segment = if (args.length > 1) args(1) else "1341690147253"

    val sc = new SparkContext(args(0), "Generate Link Reverse Site", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val s = sc.textFile("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@namin-rev/" + segment)

    for (line <- s) {
      val urls = line.split(" ")
      val uid = SiteUtils.getUid(urls(0))

      sc.parallelize(List(line)).saveAsTextFile("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@namin-live/linkrev/" + segment + "/" + uid + ".txt")
    }

    System.exit(0)
  }
}
