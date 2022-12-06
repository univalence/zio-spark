package zio.spark.sql

import zio.test._
import zio.test.Assertion._

object TryAnalysisSpec extends ZIOSpecDefault {
  val tryAnalysis: TryAnalysis[Int] = TryAnalysis(10)

  override def spec: Spec[TestEnvironment, Any] =
    suite("TryAnalysis test")(
      test("TryAnalysis can be converted to Either") {
        assert(tryAnalysis.toEither)(isRight(equalTo(10)))
      },
      test("TryAnalysis can be converted to Try") {
        assert(tryAnalysis.toTry)(isSuccess(equalTo(10)))
      },
      test("TryAnalysis can be recovered") {
        assert(tryAnalysis.recover(_ => -1))(equalTo(10))
      }
    )
}
