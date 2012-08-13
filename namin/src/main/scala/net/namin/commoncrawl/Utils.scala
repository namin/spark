package net.namin.commoncrawl

import com.google.gson._
import scala.collection.JavaConverters._

object Utils {
  val jsonParser = new JsonParser()

  def next[A,B>:Null](obj: A)(next: (A => B)): B = {
    if (obj == null) null
    else next(obj)
  }

  def default[B>:Null](obj: B)(default: B): B = {
    if (obj == null) default
    else obj
  }

  def links(linkFilter: String)(metadata: String): List[String] = {
    default {
      next[JsonObject, List[String]] (jsonParser.parse(metadata.toString).getAsJsonObject) { json =>
	next[JsonObject, List[String]] (json.getAsJsonObject("content")) { content =>
	  next[JsonArray, List[String]] (content.getAsJsonArray("links")) { links =>
	    links.iterator.asScala.map(_.getAsJsonObject).
            filter(_.get("type").getAsString == "a").
            map(_.get("href").getAsString).
            filter(x => (!x.contains("?") || !x.contains("=")) && x.contains(linkFilter)).
            toList
          }
        }
      }
    } (List())
  }
}
