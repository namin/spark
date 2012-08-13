package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

import org.apache.hadoop.io.Text
import com.google.gson._
import scala.collection.JavaConverters._

object LinkSimilarity {
  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: LinkSimilarity <host>")
      System.exit(1)
    }

    val sc = new SparkContext(args(0), "Link Similarity", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val s = sc.sequenceFile[Text,Text]("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@aws-publicdatasets/common-crawl/parse-output/segment/1341690166822/metadata-01849")

    val el = s.first()
    val md = el._2

    val jsonParser = new JsonParser()

    val json = jsonParser.parse(md.toString).getAsJsonObject
    val links = json.getAsJsonObject("content").getAsJsonArray("links").iterator.asScala.map(_.getAsJsonObject).toList
    val hrefs = links.filter(_.get("type").getAsString == "a").map(_.get("href").getAsString)

    hrefs.foreach(println)
  }
}
