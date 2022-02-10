
object Transformation {
  class Transformation[T, U](f: Dataset[T] => TryAnalysis[Dataset[U]]) {
    private def flatMap[V](f2: Dataset[U] => TryAnalysis[Dataset[V]]): Transformation[T, V] =
      new Transformation[T, V](x => TryAnalysis(f2(f(x).getOrThrowAnalysisException).getOrThrowAnalysisException))

    def selectExpr(exprs: String*): Transformation[T, Row] = flatMap(_.selectExpr(exprs: _*))
    def filter(condition: String): Transformation[T, U] = flatMap(_.filter(condition))


    def recover(f:(AnalysisException, Dataset[T]) => Dataset[U]): Dataset[T] => Dataset[U] = ???
  }

  object DataFrame extends Transformation[Row, Row](TryAnalysis.Success.apply)
}