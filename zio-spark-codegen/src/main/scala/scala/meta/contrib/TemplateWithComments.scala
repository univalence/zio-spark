package scala.meta.contrib

import scala.math.Ordering.Implicits.infixOrderingOps
import scala.meta.{Init, Position, Self, Stat, Template, Tree, Type}
import scala.meta.tokens.Token.Comment

class TemplateWithComments(template: Template, filterOverlay: Boolean) extends Template {
  val comments: AssociatedComments = AssociatedComments(template)

  private lazy val commentTokens                   = template.tokens.collect { case d: Comment => (d.value, d.pos) }
  private lazy val startTemplate: Option[Position] = commentTokens.find { case (content, _) => content.contains("template:on") }.map(_._2)
  private lazy val endTemplate: Option[Position]   = commentTokens.find { case (content, _) => content.contains("template:off") }.map(_._2)

  implicit private val ord: Ordering[Position] = Ordering.by(pos => (pos.startLine, pos.start))

  def fo[T <: Tree](lst: List[T]): List[T] =
    if (filterOverlay) {
      (startTemplate, endTemplate) match {
        case (Some(start), Some(end)) => lst.filter(_.tokens.forall(token => token.pos >= start && token.pos <= end))
        case _                        => ???
      }
    } else
      lst

  private def filtered[T <: Tree](f: Template => List[T]):List[T] = fo(f(template))

  override def derives: List[Type] = ???

  override def setDerives(_derives: List[Type]): Unit = template.setDerives(_derives)

  override def early: List[Stat] = ???

  override def inits: List[Init] = ???

  override def self: Self = template.self

  override def stats: List[Stat] = filtered(_.stats)

  override def copy(early: List[Stat], inits: List[Init], self: Self, stats: List[Stat]): Template = ???

  override def children: List[Tree] = ???

  override def productFields: List[String] = ???

  override def productElement(n: Int): Any = ???

  override def productArity: Int = ???

  private[meta] def privateCopy(
      prototype: scala.meta.Tree,
      parent: scala.meta.Tree,
      destination: String,
      origin: scala.meta.internal.trees.Origin
  ): scala.meta.Tree = ???

  private[meta] def privateOrigin: scala.meta.internal.trees.Origin = ???
  private[meta] def privateParent: scala.meta.Tree                  = ???
  private[meta] def privatePrototype: scala.meta.Tree               = ???
}
