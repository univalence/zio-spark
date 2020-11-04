package zio.spark

import zio._
import zio.test._

object SparkEnvImplicitClassTest extends DefaultRunnableSpec with SparkTest {

  val pathToto: String = "src/test/resources/toto"

  import zio.test._

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] = zio.test.suite("SparkEnv")(
    testM("sparkEven read and SparkEnv sql example") {

      val prg: ZIO[SparkEnv, Throwable, TestResult] = for {
        df   <- zio.spark.read.textFile(pathToto)
        _    <- df.execute(_.createTempView("totoview"))
        df2  <- zio.spark.sql(s"""SELECT * FROM totoview""")
        seq1 <- df.collect()
        seq2 <- df2.collect()
      } yield assert(seq1.head)(zio.test.Assertion.equalTo(seq2.head.getString(0)))

      ss >>= (x => prg.provide(Has(x)))

    } @@ max20secondes,
    testM("toDataSet") {
      val prg = for {
        ds <- zio.spark.read.textFile(pathToto)
        v  <- ds.take(1)
      } yield assert(v.head.trim)(Assertion.equalTo("bonjour"))

      ss >>= (x => prg.provide(Has(x)))

    } @@ max20secondes
  )

}
