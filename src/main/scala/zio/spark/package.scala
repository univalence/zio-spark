package zio

package object spark extends PackageSyntax {
  type SparkEnv = Has[ZSparkSession]

  type Spark[T] = RIO[SparkEnv, T]
}
