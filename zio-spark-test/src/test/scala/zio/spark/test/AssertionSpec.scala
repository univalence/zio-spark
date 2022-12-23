package zio.spark.test

import zio._
import zio.test._

object AssertionSpec extends ZIOSpecDefault {
  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("temp spec")(
      test("temp test") {
        assertTrue(1 == 1)
      }
    )
}
