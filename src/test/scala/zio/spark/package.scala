package zio

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.SparkSession
import zio.test.environment.Live
import zio.test.{ TestAspect, TestAspectAtLeastR }

package object spark {

  val ss: Task[SparkZIO] = Task(new SparkZIO(SparkSession.builder().master("local[*]").appName("toto").getOrCreate()))

  val max10s: TestAspectAtLeastR[Live] = TestAspect.timeout(zio.duration.Duration(10, TimeUnit.SECONDS))
}
