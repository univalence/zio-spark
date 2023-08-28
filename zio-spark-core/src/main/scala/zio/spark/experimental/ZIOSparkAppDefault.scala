package zio.spark.experimental

import zio._

trait ZIOSparkAppDefault extends ZIOSparkApp {
  type Environment = Any

  val bootstrap: ZLayer[ZIOAppArgs, Any, Any] = ZLayer.empty
}
