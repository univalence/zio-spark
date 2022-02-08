package zio.spark.sql

sealed trait Statistics

object Statistics {
  self =>

  case object Count                                    extends Statistics
  case object Mean                                     extends Statistics
  case object Stddev                                   extends Statistics
  case object Min                                      extends Statistics
  case object Max                                      extends Statistics
  final case class ApproximatePercentile(percent: Int) extends Statistics
  case object CountDistinct                            extends Statistics
  case object ApproximateCountDistinct                 extends Statistics

  def statisticsToString(statistics: Statistics): String =
    statistics match {
      case Count                          => "count"
      case Mean                           => "mean"
      case Stddev                         => "stddev"
      case Min                            => "min"
      case Max                            => "max"
      case ApproximatePercentile(percent) => s"$percent%"
      case CountDistinct                  => "count_distinct"
      case ApproximateCountDistinct       => "approx_count_distinct"
    }
}
