package zio

package object spark extends ServiceSyntax {
  type SparkEnv = Has[ZSparkSession]

  type SIO[T] = RIO[SparkEnv, T]

}
