package net.namin.commoncrawl

object SiteUtils {
  def getUid(url: String): String = java.util.UUID.nameUUIDFromBytes(url.getBytes).toString

  def s3save(bucketName: String, key: String, content: String) {
    try {
    val creds = new org.jets3t.service.security.AWSCredentials(System.getenv("AWS_ACCESS_KEY_ID"), System.getenv("AWS_SECRET_ACCESS_KEY"))
    val s3service = new org.jets3t.service.impl.rest.httpclient.RestS3Service(creds)
    val bucket = s3service.getBucket(bucketName)
    val obj = new org.jets3t.service.model.S3Object(key, content)
    s3service.putObject(bucket, obj)
    } catch {
      case _ => () // TODO: log?
    }
  }
}
