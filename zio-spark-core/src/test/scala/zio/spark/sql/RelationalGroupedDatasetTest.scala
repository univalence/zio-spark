package zio.spark.sql

import zio.spark.helper.Fixture._
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
import zio.spark.sql.implicits._
import zio.test._

object RelationalGroupedDatasetTest {
  final case class AggregatePerson(status: String, age: Double)

  def relationalGroupedDatasetAggregationSpec: Spec[TestConsole with SparkSession, TestFailure[Any], TestSuccess] = {
    final case class Test(
        aggregation: String,
        f:           String => RelationalGroupedDataset => DataFrame,
        expected:    Seq[Double]
    ) {
      def build: Spec[SparkSession, TestFailure[Any], TestSuccess] =
        test(s"DataFrameWriter should implement $aggregation correctly") {
          for {
            df <- readCsv(s"$resourcesPath/group.csv")
            transformedDf =
              f("age")(df.groupBy($"status"))
                .withColumnRenamed(s"$aggregation(age)", "age")
                .as[AggregatePerson]
                .map(_.age)
            output <- transformedDf.collect
          } yield assertTrue(output == expected)
        }
    }

    val tests =
      List(
        Test(
          aggregation = "avg",
          f           = col => _.mean(col),
          expected    = Seq(32.5, 58.5)
        ),
        Test(
          aggregation = "max",
          f           = col => _.max(col),
          expected    = Seq(46.0, 93.0)
        ),
        Test(
          aggregation = "min",
          f           = col => _.min(col),
          expected    = Seq(19.0, 24.0)
        ),
        Test(
          aggregation = "sum",
          f           = col => _.sum(col),
          expected    = Seq(65.0, 117.0)
        )
      )

    suite("RelationalGroupedDataset Aggregation")(tests.map(_.build): _*)
  }
}
