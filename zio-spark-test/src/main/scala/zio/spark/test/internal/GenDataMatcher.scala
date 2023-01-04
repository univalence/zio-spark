package zio.spark.test.internal

trait GenDataMatcher[T]

object GenDataMatcher {
  trait SchemaMatcher extends GenDataMatcher[Nothing]
  trait DataMatcher[T] extends GenDataMatcher[T] {
    def respect(other: T): Boolean
  }
}
