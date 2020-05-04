package zio.spark

import zio.{ Has, RIO }

package object v2 {

  type SparkEnv = Has[ZSparkSession]

  type SIO[T] = RIO[SparkEnv, T]

}
