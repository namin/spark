package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

import org.apache.hadoop.io.Text
import com.google.gson._
import scala.collection.JavaConverters._

object LinkSimilarity {
  val jsonParser = new JsonParser()

  def next[A,B>:Null](obj: A)(next: (A => B)): B = {
    if (obj == null) null
    else next(obj)
  }

  def default[B>:Null](obj: B)(default: B): B = {
    if (obj == null) default
    else obj
  }

  def links(el: (Text, Text)): List[String] = {
    val md = el._2
    default {
      next[JsonObject, List[String]] (jsonParser.parse(md.toString).getAsJsonObject) { json =>
	next[JsonObject, List[String]] (json.getAsJsonObject("content")) { content =>
	  next[JsonArray, List[String]] (content.getAsJsonArray("links")) { links =>
	    links.iterator.asScala.map(_.getAsJsonObject).
            filter(_.get("type").getAsString == "a").
            map(_.get("href").getAsString).
            toList
          }
        }
      }
    } (List())
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: LinkSimilarity <host> <urlFilter> <path>")
      System.exit(1)
    }

    val urlFilter = if (args.length > 1) args(1) else ""
    val path = if (args.length > 2) args(2) else "1341690166822/metadata-01849"

    val sc = new SparkContext(args(0), "Link Similarity", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val s = sc.sequenceFile[Text,Text]("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@aws-publicdatasets/common-crawl/parse-output/segment/" + path)

    val sf = s.filter(_._1.toString contains urlFilter)

    val el = sf.first()
    val hrefs = links(el)
    hrefs.foreach(println)
  }
}
