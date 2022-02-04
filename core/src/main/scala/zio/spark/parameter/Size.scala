package zio.spark.parameter

sealed trait Size

object Size {

  /** Converts the Size into its String representation. */
  def sizeToString(size: Size): String =
    size match {
      case Unlimited   => "0"
      case Byte(v)     => s"${v}b"
      case KibiByte(v) => s"${v}kb"
      case MebiByte(v) => s"${v}mb"
      case GibiByte(v) => s"${v}gb"
      case TebiByte(v) => s"${v}tb"
      case PebiByte(v) => s"${v}pb"
    }

  final case class Byte(amount: Int) extends Size

  final case class KibiByte(amount: Int) extends Size

  final case class MebiByte(amount: Int) extends Size

  final case class GibiByte(amount: Int) extends Size

  final case class TebiByte(amount: Int) extends Size

  final case class PebiByte(amount: Int) extends Size

  final case object Unlimited extends Size
}
