package zio.spark.sql

object ExtraDatasetCompatibilityMethods {
  val extraAllowedMethods: Seq[String] = Seq("tailOption", "takeRight", "lastOption", "last")
}
