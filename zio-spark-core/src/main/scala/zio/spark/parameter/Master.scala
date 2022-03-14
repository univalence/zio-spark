package zio.spark.parameter

sealed trait Master {
  self =>

  import Master._

  override def toString: String =
    self match {
      case Local(nWorkers)                          => s"local[$nWorkers]"
      case LocalWithFailures(nWorkers, maxFailures) => s"local[$nWorkers,$maxFailures]"
      case LocalAllNodes                            => "local[*]"
      case LocalAllNodesWithFailures(maxFailures)   => s"local[*,$maxFailures]"
      case Spark(masters) =>
        val masterUrls = masters.map(_.toSparkString).mkString(",")
        s"spark://$masterUrls"
      case Mesos(master) => s"mesos://${master.toSparkString}"
      case Yarn          => "yarn"
    }
}

object Master {
  final case class MasterNodeConfiguration(host: String, port: Int) {
    def toSparkString: String = s"$host:$port"
  }

  final case class Local(nWorkers: Int) extends Master

  final case class LocalWithFailures(nWorkers: Int, maxFailures: Int) extends Master

  final case class LocalAllNodesWithFailures(maxFailures: Int) extends Master

  final case class Spark(masters: List[MasterNodeConfiguration]) extends Master

  final case class Mesos(master: MasterNodeConfiguration) extends Master

  case object LocalAllNodes extends Master

  case object Yarn extends Master
}
