package io.univalence.sparkzio

import io.univalence.sparkzio.SparkEnv.TaskS
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.scalatest.FunSuite
import zio.{ DefaultRuntime, IO, Task, ZIO }
import SparkEnv.implicits._

class SparkEnvImplicitClassTest extends FunSuite {
  val runtime: DefaultRuntime  = new DefaultRuntime {}
  val sparkZIO: Task[SparkZIO] = Task(SparkSession.builder.master("local[*]").getOrCreate()).map(x => new SparkZIO(x))
  val pathToto: String         = "spark-zio/src/test/resources/toto"

  test("sparkEnv read and SparkEnv sql example") {

    val prg: TaskS[(DataFrame, DataFrame)] = for {
      df  <- SparkEnv.read.textFile(pathToto)
      _   <- Task(df.createTempView("totoview"))
      df2 <- SparkEnv.sql(s"""SELECT * FROM totoview""")
      //_  <- df.zwrite.text("totoWriteZIO")
    } yield (df, df2)

    //Providing SparkEnv to ZIO
    val liveProgram: IO[Throwable, (DataFrame, DataFrame)] = sparkZIO.flatMap(prg.provide)

    //Unsafe run
    val resRun: (DataFrame, DataFrame) = runtime.unsafeRun(liveProgram)
    println("toto")

    assert(resRun._1.collect() sameElements resRun._2.collect())
  }

  test("ZIO catchAll example") {
    val tigrou: String = "tigrou will only appear in success"

    val prgSuccess: Task[Int] = for {
      _ <- Task(println("Program with no Exception: "))
      code <- Task {
        for {
          successCode <- Task(1)
          _           <- Task(println(tigrou))
        } yield successCode
      }.flatten.catchAll {
        case e: Exception =>
          for {
            _ <- Task {
              print("Error: ")
              println(e.getMessage)
            }
            errorCode <- Task(-1)
          } yield errorCode
      }
    } yield code

    val prgFail: Task[Int] = for {
      _ <- Task(println("Program with Exception: "))
      code <- Task {
        for {
          successCode <- Task(1 / 0)
          _           <- Task(println(tigrou))
        } yield successCode
      }.flatten.catchAll {
        case e: Exception =>
          for {
            _ <- Task {
              print("Exception: ")
              println(e.getMessage)
            }
            errorCode <- Task(-1)
          } yield errorCode
      }
    } yield code

    assert(runtime.unsafeRun(prgSuccess) === 1)
    assert(runtime.unsafeRun(prgFail) === -1)
  }

  /*test("sometimes you will use collectAll") {
    val primeSeq: Seq[Int] = Seq(2, 3, 5, 7, 11)

    for {
      ss <- SparkEnv.sparkSession
      seqDF <- {
        import ss.implicits._
        Task(
          primeSeq.map { primeNumber =>
            for {
              df <- sparkEnv.read.textFile(pathToto)
            } yield df.withColumn("prime", 'primeNumber)
          }
        )
      }
    } yield seqDF

  }*/

}
