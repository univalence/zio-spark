package zio.spark.sql

import scala3encoders.given // scalafix:ok

import zio.spark.helper.Fixture._
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
import zio.spark.sql.implicits._
import zio.spark.test._
import zio.test._

object RelationalGroupedDatasetSpec extends ZIOSparkSpecDefault {
  final case class AggregatePerson(status: String, age: Double)

  def relationalGroupedDatasetAggregationSpec: Spec[SparkSession, Any] = {
    final case class Test(
        aggregation: String,
        f:           String => RelationalGroupedDataset => DataFrame,
        expected:    Map[String, Double]
    ) {
      def build: Spec[SparkSession, Any] =
        test(s"DataFrameWriter should implement $aggregation correctly") {
          for {
            df <- readCsv(s"$resourcesPath/group.csv")
            transformedDf =
              f("age")(df.groupBy($"status"))
                .withColumnRenamed(s"$aggregation(age)", "age")
                .as[AggregatePerson]
            output <- transformedDf.collect
            obtained = output.map(p => p.status -> p.age).toMap
          } yield assertTrue(obtained == expected)
        }
    }

    val tests =
      List(
        Test(
          aggregation = "avg",
          f           = col => _.mean(col),
          expected    = Map("teacher" -> 32.5, "student" -> 58.5)
        ),
        Test(
          aggregation = "max",
          f           = col => _.max(col),
          expected    = Map("teacher" -> 46.0, "student" -> 93.0)
        ),
        Test(
          aggregation = "min",
          f           = col => _.min(col),
          expected    = Map("teacher" -> 19.0, "student" -> 24.0)
        ),
        Test(
          aggregation = "sum",
          f           = col => _.sum(col),
          expected    = Map("teacher" -> 65.0, "student" -> 117.0)
        )
      )

    suite("RelationalGroupedDataset Aggregation")(tests.map(_.build): _*)
  }

  override def spec: Spec[SparkSession, Any] = relationalGroupedDatasetAggregationSpec
}
