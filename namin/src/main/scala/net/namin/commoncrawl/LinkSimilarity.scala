package net.namin.commoncrawl

import spark.SparkContext
import spark.SparkContext._

import org.apache.hadoop.io.Text
import com.google.gson._
import scala.collection.JavaConverters._
import com.google.common.net.InternetDomainName
import java.net.URI;

object LinkSimilarity {
  val jsonParser = new JsonParser()

  def domain(url: String): Option[String] = {
    val uri = new URI(url)
    val host = uri.getHost
    if (host == null) return None

    val domainObj = InternetDomainName.from(host)
    val domain = domainObj.topPrivateDomain.name

    if (domain == null) return None

    Some(domain)
  }

  def next[A,B>:Null](obj: A)(next: (A => B)): B = {
    if (obj == null) null
    else next(obj)
  }

  def default[B>:Null](obj: B)(default: B): B = {
    if (obj == null) default
    else obj
  }

  def links(el: (Text, Text)): List[String] = {
    val elDomain = domain(el._1.toString)
    if (elDomain == None) return List()
    val differentDomain = { dom: Option[String] =>
      dom match {
	case None => false
	case Some(d) => d != elDomain.get
      }
    }
    val md = el._2
    default {
      next[JsonObject, List[String]] (jsonParser.parse(md.toString).getAsJsonObject) { json =>
	next[JsonObject, List[String]] (json.getAsJsonObject("content")) { content =>
	  next[JsonArray, List[String]] (content.getAsJsonArray("links")) { links =>
	    links.iterator.asScala.map(_.getAsJsonObject).
            filter(_.get("type").getAsString == "a").
            map(_.get("href").getAsString).
            filter(x => differentDomain(domain(x))).
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

    val sf = if (urlFilter == "") s else s.filter(_._1.toString contains urlFilter)

    val g = (for (el <- sf; url = el._1.toString; lnks = links(el); a <- lnks; b <- lnks; if a != b) yield (a, b)).groupByKey

    g.foreach(println)
  }
}
