package zio

package object spark {
  type SparkEnv = Has[ZSparkSession]

  type SIO[T] = RIO[SparkEnv, T]

  def sql(queryString: String): SIO[ZDataFrame] = ZIO.accessM(_.get.sql(queryString))

  def read: Read = {
    case class ReadImpl(read: SIO[ZSparkSession#Read]) extends Read {
      override def option(key: String, value: String): Read = copy(read.map(_.option(key, value)))

      override def parquet(path: String): SIO[ZDataFrame]        = read.flatMap(_.parquet(path))
      override def textFile(path: String): SIO[ZDataset[String]] = read.flatMap(_.textFile(path))
    }

    ReadImpl(sparkSession.map(_.read))
  }

  def sparkSession: SIO[ZSparkSession] = ZIO.access(_.get)

  trait Read {
    def option(key: String, value: String): Read
    def parquet(path: String): SIO[ZDataFrame]
    def textFile(path: String): SIO[ZDataset[String]]
  }

}
