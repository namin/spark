package net.namin.commoncrawl

object SiteUtils {
  def getUid(url: String): String = java.util.UUID.nameUUIDFromBytes(url.getBytes).toString
}
