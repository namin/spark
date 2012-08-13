package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

import com.google.gson._
import scala.collection.JavaConverters._

import com.google.common.net.InternetDomainName
import java.net.URI;

object GenerateSite {
  val jsonParser = new JsonParser()

  def domain(url: String): String = {
    val uri = new URI(url)
    val host = uri.getHost
    val domainObj = InternetDomainName.from(host)
    val domain = domainObj.topPrivateDomain.name
    domain
  }

  def parseLine(line: String): Entry = {
    val json = jsonParser.parse("{" + line.substring(0, line.length-1) + "}").getAsJsonObject
    val e = json.entrySet.asScala.first
    val url = e.getKey
    val details =  e.getValue.getAsJsonArray.iterator.asScala.map({y =>
      val x = y.getAsJsonObject.entrySet.asScala.first
      (x.getKey, x.getValue.getAsInt)
    }).toList
    Entry(url, details)
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: GenerateSite <host> <segment>")
      System.exit(1)
    }

    val segment = if (args.length > 1) args(1) else "1341690147253"

    val sc = new SparkContext(args(0), "Generate Site", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val s = sc.textFile("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@namin-sim/" + segment)

    val line = s.first()
    println(parseLine(line))
  }

  case class Entry(url: String, details: List[(String, Int)])
}
