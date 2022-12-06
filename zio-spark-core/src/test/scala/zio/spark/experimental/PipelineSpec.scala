package zio.spark.experimental

import zio.Task
import zio.spark.ZioSparkTestSpec.SparkTestSpec
import zio.spark.helper.Fixture._
import zio.spark.sql._
import zio.test._
import zio.test.Assertion._

object PipelineSpec {
  def pipelineSpec: SparkTestSpec =
    suite("Pipeline Spec")(
      test("Pipeline can be build without transformations") {
        val write: DataFrame => Task[Long] = _.count

        val pipeline = Pipeline.buildWithoutTransformation(read)(write)

        pipeline.run.map(assert(_)(equalTo(4L)))
      },
      test("Pipeline can be build with transformations") {
        val write: DataFrame => Task[Long]  = _.count
        val process: DataFrame => DataFrame = _.limit(2)

        val pipeline = Pipeline.build(read)(process)(write)

        pipeline.run.map(assert(_)(equalTo(2L)))
      }
    )
}
