package zio.spark.sql

import org.apache.spark.sql.{SparkSession => UnderlyingSparkSession}
import zio._
import zio.test.Assertion._
import zio.test.TestAspect._
import zio.test._

object DataFrameTest extends DefaultRunnableSpec {
  def spec =
    suite("HelloWorldSpec")(
      test("DataFrame should implement count correctly") {
        val session: UnderlyingSparkSession =
          UnderlyingSparkSession
            .builder()
            .master("local[*]")
            .appName("zio-spark")
            .getOrCreate()

        val df = session.read.csv("src/test/resources/data.csv")

        val count = df.count()

        assert(count)(equalTo(4L))
      } @@ ignore,
      test("ZDataFrame should implement count correctly") {
        val session =
          ZSparkSession
            .builder()
            .master("local[*]")
            .appName("zio-spark")
            .getOrCreateLayer()

        val pipeline =
          Spark(
            input   = _.read.csv("src/test/resources/data.csv"),
            process = df => df,
            output  = _.count()
          )

        val job = pipeline.provideLayer(session)

        job.map(assert(_)(equalTo(4L)))
      } @@ timeout(60.seconds)
    )
}
