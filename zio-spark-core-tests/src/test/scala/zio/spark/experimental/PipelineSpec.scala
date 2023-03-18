package zio.spark.experimental

import zio.Task
import zio.spark.helper.Fixture._
import zio.spark.sql._
import zio.spark.test._
import zio.test._

object PipelineSpec extends ZIOSparkSpecDefault {
  override def spec: Spec[SparkSession, Throwable] =
    suite("Pipeline spec")(
      test("Pipeline can be build without transformations") {
        val write: DataFrame => Task[Long] = _.count

        val pipeline = Pipeline.buildWithoutTransformation(read)(write)

        pipeline.run.map(output => assertTrue(output == 4L))
      },
      test("Pipeline can be build with transformations") {
        val write: DataFrame => Task[Long]  = _.count
        val process: DataFrame => DataFrame = _.limit(2)

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(output => assertTrue(output == 2L))
      }
    )
}
