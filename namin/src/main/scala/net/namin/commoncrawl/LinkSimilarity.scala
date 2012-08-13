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
    try {
      val uri = new URI(url)
      val host = uri.getHost
      if (host == null) return None

      val domainObj = InternetDomainName.from(host)
      val domain = domainObj.topPrivateDomain.name

      if (domain == null) return None

      Some(domain)
    } catch {
      case _ => None
    }
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
            filter(x => differentDomain(domain(x)) && !x.contains("?")).
            toList
          }
        }
      }
    } (List())
  }

  def main(args: Array[String]) {
    if (args.length == 0) {
      System.err.println("Usage: LinkSimilarity <host> <urlFilter> <segment> <metadata>")
      System.exit(1)
    }

    val urlFilter = if (args.length > 1) args(1) else ""
    // To get the list of valid segments with Tim Kay's aws tool:
    // aws ls -1 aws-publicdatasets/common-crawl/parse-output/valid_segments/ | cut -d / -f 4
    val segment = if (args.length > 2) args(2) else "1341690166822"
    val metadata = if (args.length > 3) args(3) else "metadata-*" //"metadata-01849"
    val path = segment + "/" + metadata

    val sc = new SparkContext(args(0), "Link Similarity", System.getenv("SPARK_HOME"), List(System.getenv("SPARK_NAMIN_JAR")))

    val s = sc.sequenceFile[Text,Text]("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@aws-publicdatasets/common-crawl/parse-output/segment/" + path)

    val sf = if (urlFilter == "") s else s.filter(_._1.toString contains urlFilter)

    val g = (for (el <- sf; url = el._1.toString; lnks = links(el); a <- lnks; b <- lnks; if a != b) yield (a, b)).groupByKey

    val h = g.map({ case (key, refs) =>
      val bestRefs = refs.groupBy(x => x).map({case (ref, seq) =>
        (ref, seq.size)
      }).filter({case (ref, c) =>
        c > 1
      })
      (key, bestRefs)
    }).filter(!_._2.isEmpty)

    h.foreach(println)

    val i = h.map({case (key, m) => MyMap(key, m)})

    i.saveAsTextFile("s3n://" + System.getenv("AWS_ACCESS_KEY_ID") + ":" + System.getenv("AWS_SECRET_ACCESS_KEY") + "@namin-sim/" + segment + "/")

    System.exit(0)
  }

  case class MyMap(val key: String, val m: Map[String, Int]) {
    override def toString = {
      "\"" + key + "\"" + " : " +
      m.toSeq.sortBy({ case (ref, c) =>
        -c
      }).map({case (ref, c) =>
        "{\"" + ref + "\" : " + c + "}"
      }).mkString("[", ",", "]") + ","
    }
  }
}
