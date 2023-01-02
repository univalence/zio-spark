package zio.spark.test

import scala.reflect.macros._

// Pilfered (with immense gratitude & minor modifications)
// from https://github.com/zio/zio/blob/series/2.x/test/shared/src/main/scala-2/zio/test/Macros.scala

// scalafix:off
private[test] object Macros {
  def assert_impl(c: blackbox.Context)(value: c.Tree)(assertion: c.Tree): c.Tree = {
    import c.universe._

    // Pilfered (with immense gratitude & minor modifications)
    // from https://github.com/com-lihaoyi/sourcecode
    def text[T: c.WeakTypeTag](tree: c.Tree): (Int, Int, String) = {
      val fileContent = new String(tree.pos.source.content)

      var start =
        tree.collect { case treeVal =>
          treeVal.pos match {
            case NoPosition => Int.MaxValue
            case p          => p.start
          }
        }.min
      val initialStart = start

      // Moves to the true beginning of the expression, in the case where the
      // internal expression is wrapped in parens.
      while ((start - 2) >= 0 && fileContent(start - 2) == '(')
        start -= 1

      val g      = c.asInstanceOf[reflect.macros.runtime.Context].global
      val parser = g.newUnitParser(fileContent.drop(start))
      parser.expr()
      val end = parser.in.lastOffset
      (initialStart - start, start, fileContent.slice(start, start + end))
    }

    val codeString      = text(value)._3
    val assertionString = text(assertion)._3
    q"_root_.zio.spark.test.assertSparkImpl($value, $codeString, $assertionString)($assertion)"
  }

  def assertZIO_impl(c: blackbox.Context)(value: c.Tree)(assertion: c.Tree): c.Tree = {
    import c.universe._

    // Pilfered (with immense gratitude & minor modifications)
    // from https://github.com/com-lihaoyi/sourcecode
    def text[T: c.WeakTypeTag](tree: c.Tree): (Int, Int, String) = {
      val fileContent = new String(tree.pos.source.content)

      var start =
        tree.collect { case treeVal =>
          treeVal.pos match {
            case NoPosition => Int.MaxValue
            case p          => p.start
          }
        }.min
      val initialStart = start

      // Moves to the true beginning of the expression, in the case where the
      // internal expression is wrapped in parens.
      while ((start - 2) >= 0 && fileContent(start - 2) == '(')
        start -= 1

      val g      = c.asInstanceOf[reflect.macros.runtime.Context].global
      val parser = g.newUnitParser(fileContent.drop(start))
      parser.expr()
      val end = parser.in.lastOffset
      (initialStart - start, start, fileContent.slice(start, start + end))
    }

    val codeString      = text(value)._3
    val assertionString = text(assertion)._3
    q"_root_.zio.spark.test.assertZIOSparkImpl($value, $codeString, $assertionString)($assertion)"
  }
}
