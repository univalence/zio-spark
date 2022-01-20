package zio.spark.sql

import zio._

object Spark {
  type Pipeline[Out] = ZIO[SparkSession, Throwable, Out]

  def apply[Out](
      input: SparkSession => Task[DataFrame],
      process: DataFrame => DataFrame,
      output: DataFrame => Task[Out]
  ): Pipeline[Out] =
    for {
      session <- ZIO.service[SparkSession]
      df      <- input(session)
      processed = process(df)
      value <- output(processed)
    } yield value
}
