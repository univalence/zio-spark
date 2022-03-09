package zio.spark.sql

import zio.test._

object TryAnalysisTest extends DefaultRunnableSpec {
  val tryAnalysis: TryAnalysis[Int] = TryAnalysis(10)

  override def spec: ZSpec[TestEnvironment, Any] =
    suite("TryAnalysis test")(
      test("TryAnalysis can be converted to Either") {
        assertTrue(tryAnalysis.toEither == Right(10))
      },
      test("TryAnalysis can be converted to Try") {
        assertTrue(tryAnalysis.toTry == scala.util.Success(10))
      },
      test("TryAnalysis can be recovered") {
        assertTrue(tryAnalysis.recover(_ => -1) == 10)
      }
    )
}
