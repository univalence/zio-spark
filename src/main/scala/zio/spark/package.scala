package zio

package object spark extends PackageSyntax {
  type SparkEnv = Has[ZSparkSession]

  type SIO[T] = RIO[SparkEnv, T]

}
