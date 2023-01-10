package zio.spark.test
import zio.Scope
import zio.spark.sql.SparkSession
import zio.test.{Spec, TestEnvironment}
import zio.test.TestAspect.failing

// scalafix.ok
object ExpectSpec extends SharedZIOSparkSpecDefault {

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
        Dataset(1, 2, 3).flatMap(_.expectAll(row((_: Int) > 0).allowMultipleMatches))
      },
      test("Dataset should validate expect all with or conditional match") {
        val condition1: Int => Boolean = _ > 1
        val condition2: Int => Boolean = _ == 1
        Dataset(1, 2, 3).flatMap(_.expectAll(row(condition1 || condition2).allowMultipleMatches))
      },
      test("Dataset should validate expect all with and conditional match") {
        val condition1: Int => Boolean = _ > 0
        val condition2: Int => Boolean = _ <= 3
        Dataset(1, 2, 3).flatMap(_.expectAll(row(condition1 && condition2).allowMultipleMatches))
      },
      test("Dataset should fail expect all if wrong conditional match") {
        Dataset(1, 2, 3).flatMap(_.expectAll(row((_: Int) > 1).allowMultipleMatches))
      } @@ failing,
      test("Dataframe should validate expect all with exact row match") {
        for {
          people <- Dataset(Person("Louis", 50), Person("Lara", 26))
          result <- people.toDF.expectAll(row("Louis", 50), row("Lara", 26))
        } yield result
      },
      test("Dataframe should fail expect all with if missing row match") {
        for {
          people <- Dataset(Person("Louis", 50), Person("Lara", 26))
          result <- people.toDF.expectAll(row("Louis", 50))
        } yield result
      } @@ failing,
      test("Dataframe should validate expect all with __ in it") {
        for {
          people <- Dataset(Person("Louis", 50), Person("Lara", 50))
          result <- people.toDF.expectAll(row(__, 50).allowMultipleMatches)
        } yield result
      },
      test("Dataframe should fail expect all with __ in it but wrong row match") {
        for {
          people <- Dataset(Person("Louis", 50), Person("Lara", 25))
          result <- people.toDF.expectAll(row(__, 50))
        } yield result
      } @@ failing,
      test("Dataframe should handle schema") {
        for {
          people <- Dataset(Person("Louis", 50), Person("Lara", 50))
          result <- people.toDF.expectAll(schema("age"), row(50).allowMultipleMatches)
        } yield result
      },
      test("Dataframe should handle schema permutations") {
        for {
          people <- Dataset(Person("Louis", 50), Person("Lara", 50))
          result <- people.toDF.expectAll(schema("age", "name"), row(50, __).allowMultipleMatches)
        } yield result
      }
    )
}
