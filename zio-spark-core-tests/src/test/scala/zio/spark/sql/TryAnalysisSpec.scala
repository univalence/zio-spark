package zio.spark.sql

import zio.test._

import scala.util.Success

object TryAnalysisSpec extends ZIOSpecDefault {
  val tryAnalysis: TryAnalysis[Int] = TryAnalysis(10)

  override def spec: Spec[TestEnvironment, Any] =
    suite("TryAnalysis test")(
      test("TryAnalysis can be converted to Either") {
        assertTrue(tryAnalysis.toEither == Right(10))
      },
      test("TryAnalysis can be converted to Try") {
        assertTrue(tryAnalysis.toTry == Success(10))
      },
      test("TryAnalysis can be recovered") {
        assertTrue(tryAnalysis.recover(_ => -1) == 10)
      }
    )
}
