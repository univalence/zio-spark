package zio.spark

/** The parameter package provides helpers to configure a Spark job. */
package object parameter {

  /**
   * Extension methods to transform an int into a [[Size]].
   * ==Overview==
   * {{{
   *   scala> val size = 2.bytes
   *   size: zio.spark.parameter.Size.Byte(2)
   * }}}
   */
  implicit class IntSize(val i: Int) {
    def bytes: Size = i.byte

    def byte: Size = Size.Byte(i)

    def b: Size = i.byte

    def kibibytes: Size = i.kibibyte

    def kibibyte: Size = Size.KibiByte(i)

    def kb: Size = i.kibibyte

    def mebibytes: Size = i.mebibyte

    def mb: Size = i.mebibyte

    def mebibyte: Size = Size.MebiByte(i)

    def gibibytes: Size = i.gibibyte

    def gb: Size = i.gibibyte

    def gibibyte: Size = Size.GibiByte(i)

    def tebibytes: Size = i.tebibyte

    def tb: Size = i.tebibyte

    def tebibyte: Size = Size.TebiByte(i)

    def pebibytes: Size = i.pebibyte

    def pebibyte: Size = Size.PebiByte(i)

    def pb: Size = i.pebibyte
  }

  val unlimitedSize: Size = Size.Unlimited

  val url: (String, Int) => Master.MasterNodeConfiguration =
    (host: String, port: Int) => Master.MasterNodeConfiguration(host, port)

  val localAllNodes: Master                   = Master.LocalAllNodes
  val local: Int => Master                    = (nodes: Int) => Master.Local(nodes)
  val localWithFailures: (Int, Int) => Master = (nodes: Int, failures: Int) => Master.LocalWithFailures(nodes, failures)
  val localAllNodesWithFailures: Int => Master = (failures: Int) => Master.LocalAllNodesWithFailures(failures)
  val yarn: Master                             = Master.Yarn
  val spark: List[Master.MasterNodeConfiguration] => Master =
    (urls: List[Master.MasterNodeConfiguration]) => Master.Spark(urls)
  val mesos: Master.MasterNodeConfiguration => Master = (url: Master.MasterNodeConfiguration) => Master.Mesos(url)
}
