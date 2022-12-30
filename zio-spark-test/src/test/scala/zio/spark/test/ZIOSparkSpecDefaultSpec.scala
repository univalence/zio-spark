package zio.spark.test

import scala3encoders.given // scalafix:ok

import zio.spark.sql.implicits._
import zio.spark.test.SparkAssertion._
import zio.test._

/**
 * What we have :
 * Assertion failed:
  ✗ false was not true
  Dataset was not empty
  Dataset(1, 2, 3) did not satisfy isEmpty
  Dataset(1, 2, 3) = false
  at ...
 */

/**
 * What we want :
 * Assertion failed:
  ✗ Dataset was not empty
  Dataset(1, 2, 3) did not satisfy isEmpty
  Dataset(1, 2, 3).isEmpty = false
  at ...
 */

/**
 * Assertion failed: ✗ 3 was not equal to 2 count did not satisfy
 * Assertion.equalTo(2L) => dataset did not satisfy isEmpty count = 3
 * check
 */
object ZIOSparkSpecDefaultSpec extends ZIOSparkSpecDefault {
  Assertion
  override def sparkSpec =
    suite("ZIOSparkSpecDefault can run spark job without providing layer")(
      test("It can run Dataset job") {
        assertZIOSpark(Dataset(1, 2, 3))(isEmpty)
      },
      test("It can run RDD job") {
        for {
          rdd <- RDD(1, 2, 3)
          res <- assertZIO(rdd.count)(Assertion.equalTo(2L))
        } yield res
      }
    )
}
