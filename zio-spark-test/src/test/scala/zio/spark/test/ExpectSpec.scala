package zio.spark.test
import zio.Scope
import zio.spark.sql.SparkSession
import zio.test.{Spec, TestEnvironment}
import zio.test.TestAspect.failing
object ExpectSpec extends SharedZIOSparkSpecDefault { // scalafix.ok

  import zio.spark.sql.implicits._

  final case class Person(name: String, age: Int)

  override def spec: Spec[SparkSession with TestEnvironment with Scope, Any] =
    suite("Expect spec")(
      test("Dataset should validate expect all with exact data match") {
        Dataset(1, 2, 3).flatMap(_.expectAll(row(1), row(2), row(3)))
      },
      test("Dataset should fail expect all if missing data match") {
        Dataset(1, 2, 3).flatMap(_.expectAll(row(1), row(2)))
      } @@ failing,
      test("Dataset should validate expect all with conditional match") {
        Dataset(1, 2, 3).flatMap(_.expectAll(row(_ > 0)))
      },
      test("Dataset should fail expect all if wrong conditional match") {
        Dataset(1, 2, 3).flatMap(_.expectAll(row(_ > 1)))
      } @@ failing,
      test("Dataframe should validate expect all with exact row match") {
        val people =
          Dataset(
            Person("Louis", 50),
            Person("Oliver", 26),
            Person("Lara", 43)
          )

        people
          .map(_.toDF)
          .flatMap { df =>
            df.expectAll(
              row("Louis", 50),
              row("Oliver", 26),
              row("Lara", 43)
            )
          }
      },
      test("Dataframe should fail expect all with if missing row match") {
        val people =
          Dataset(
            Person("Louis", 50),
            Person("Oliver", 26),
            Person("Lara", 43)
          )

        people
          .map(_.toDF)
          .flatMap { df =>
            df.expectAll(
              row("Louis", 50),
              row("Oliver", 26)
            )
          }
      } @@ failing
    )
}
