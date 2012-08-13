package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

import org.apache.hadoop.io.Text

object LinkReverse {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: LinkReverse <host> <urlFilter> <segment> <metadata>")
      System.exit(1)
    }

    val urlFilter = if (args.length > 1) args(1) else ""
    val segment = if (args.length > 2) args(2) else "1341690166822"
    val metadata = if (args.length > 3) args(3) else "metadata-01849"

    val sc = new SparkContext(args(0), "Link Similarity", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val g = go(sc, urlFilter, segment, metadata)

    g.saveAsTextFile("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@namin-rev/" + segment + "/")

    System.exit(0)
  }

  def go(sc: SparkContext, urlFilter: String, segment: String, metadata: String) = {
    val path = segment + "/" + metadata

    val s = sc.sequenceFile[Text,Text]("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@aws-publicdatasets/common-crawl/parse-output/segment/" + path)

    val g =
      (for (el <- s; fromUrl = el._1.toString; md = el._2.toString;
            lnks = Utils.links(urlFilter)(md); toUrl <- lnks)
       yield (toUrl, fromUrl)).groupByKey.sortByKey(true)

    g.map(x => Entry(x._1,x._2.sorted))
  }

  case class Entry(toUrl: String, fromUrls: Seq[String]) {
    override def toString = {
      toUrl + " " + fromUrls.mkString(" ")
    }
  }
}
