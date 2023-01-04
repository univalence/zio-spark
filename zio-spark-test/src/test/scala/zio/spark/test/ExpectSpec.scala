package zio.spark.test
import zio.Scope
import zio.spark.sql.SparkSession
import zio.test.{Spec, TestEnvironment}
import scala3encoders.given // scalafix:ok

import zio.spark.sql.implicits._
object ExpectSpec extends SharedZIOSparkSpecDefault {
  override def spec: Spec[SparkSession with TestEnvironment with Scope, Any] =
    suite("Expect spec")(
      test("Dataframe should implement expect all") {
        Dataset(1, 2, 3).expectAll(row(1), row(2), row(3))
      }
    )

}
