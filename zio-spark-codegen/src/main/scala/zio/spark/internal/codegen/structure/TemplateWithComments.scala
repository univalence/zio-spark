package zio.spark.internal.codegen.structure

import scala.math.Ordering.Implicits.infixOrderingOps
import scala.meta.contrib.AssociatedComments
import scala.meta.tokens.Token.Comment
import scala.meta.{Position, Stat, Template, Tree}

class TemplateWithComments(template: Template, filterOverlay: Boolean)  {
  val comments: AssociatedComments = AssociatedComments(template)

  private lazy val commentTokens                   = template.tokens.collect { case d: Comment => (d.value, d.pos) }
  private lazy val startTemplate: Option[Position] = commentTokens.find { case (content, _) => content.contains("template:on") }.map(_._2)
  private lazy val endTemplate: Option[Position]   = commentTokens.find { case (content, _) => content.contains("template:off") }.map(_._2)

  implicit private val ord: Ordering[Position] = Ordering.by(pos => (pos.startLine, pos.start))

  private def fo[T <: Tree](lst: List[T]): List[T] =
    if (filterOverlay) {
      (startTemplate, endTemplate) match {
        case (Some(start), Some(end)) => lst.filter(_.tokens.forall(token => token.pos >= start && token.pos <= end))
        case _                        => ???
      }
    } else
      lst

  private def filtered[T <: Tree](f: Template => List[T]):List[T] = fo(f(template))

  def stats: List[Stat] = filtered(_.stats)
}
