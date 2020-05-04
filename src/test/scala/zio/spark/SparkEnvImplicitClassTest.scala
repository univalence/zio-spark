package zio.spark

import org.apache.spark.sql.SparkSession
import zio._
import zio.test._

class SparkEnvImplicitClassTest extends DefaultRunnableSpec {

  val sparkZIO: Task[SparkZIO] = Task(SparkSession.builder.master("local[*]").getOrCreate()).map(x => new SparkZIO(x))
  val pathToto: String         = "src/test/resources/toto"

  import zio.test._

  override def spec: ZSpec[zio.test.environment.TestEnvironment, Any] = zio.test.suite("SparkEnv")(
    testM("sparkEven read and SparkEnv sql example") {
      val prg: ZIO[SparkEnv, Throwable, TestResult] = for {
        df  <- SparkEnv.read.textFile(pathToto)
        _   <- Task(df.createTempView("totoview"))
        df2 <- SparkEnv.sql(s"""SELECT * FROM totoview""")
      } yield assert(df.collect().toSeq)(zio.test.Assertion.equalTo(df2.collect().toSeq))

      sparkZIO.flatMap(prg.provide)
    }
  )

}
