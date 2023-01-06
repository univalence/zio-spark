package zio.spark.test

import scala3encoders.given // scalafix:ok

import zio.spark.sql.implicits._
import zio.spark.test.SparkAssertion._
import zio.test.TestAspect.failing

object SparkAssertionSpec extends SharedZIOSparkSpecDefault {

  override def spec =
    suite("SparkAssertion spec")(
      test("Assertions should work with assertSpark") {
        for {
          df     <- Dataset[Int]()
          result <- assertSpark(df)(isEmpty)
        } yield result
      },
      test("It should assert that a dataset is empty") {
        assertZIOSpark(Dataset[Int]())(isEmpty)
      },
      test("It should fail asserting that a dataset is empty") {
        assertZIOSpark(Dataset(1, 2, 3))(isEmpty)
      } @@ failing,
      test("It should assert that at least a row respect the predicate") {
        assertZIOSpark(Dataset(1, 2, 3))(shouldExist("value == 1"))
      },
      test("It should fail asserting that at least a row respect the predicate") {
        assertZIOSpark(Dataset(1, 2, 3))(shouldExist("value == 4"))
      } @@ failing,
      test("It should assert that no rows respect the predicate") {
        assertZIOSpark(Dataset(1, 2, 3))(shouldNotExist("value == 4"))
      },
      test("It should fail asserting that no rows respect the predicate") {
        assertZIOSpark(Dataset(1, 2, 3))(shouldNotExist("value == 1"))
      } @@ failing
    )
}
