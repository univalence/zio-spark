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
        val people = Dataset(Person("Louis", 50), Person("Lara", 26))

        people.map(_.toDF).flatMap(_.expectAll(row("Louis", 50), row("Lara", 26)))
      },
      test("Dataframe should fail expect all with if missing row match") {
        val people = Dataset(Person("Louis", 50), Person("Lara", 26))

        people.map(_.toDF).flatMap(_.expectAll(row("Louis", 50)))
      } @@ failing,
      test("Dataframe should validate expect all with __ in it") {
        val people = Dataset(Person("Louis", 50), Person("Lara", 50))

        people.map(_.toDF).flatMap(_.expectAll(row(__, 50)))
      },
      test("Dataframe should fail expect all with __ in it but wrong row match") {
        val people = Dataset(Person("Louis", 50), Person("Lara", 25))

        people.map(_.toDF).flatMap(_.expectAll(row(__, 50)))
      } @@ failing,
    )
}
