package zio.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import zio.Task
import zio.spark.wrap.Clean
import zio.test.environment.Live
import zio.test.{ TestAspect, TestAspectAtLeastR }

trait SparkTest {

  val ss: Task[ZSparkSession] = Clean.effect(SparkSession.builder().master("local[*]").appName("toto").getOrCreate())

  val max20secondes: TestAspectAtLeastR[Live] = TestAspect.timeout(zio.duration.Duration(20, TimeUnit.SECONDS))
}
