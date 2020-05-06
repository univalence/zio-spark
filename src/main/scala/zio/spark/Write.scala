package zio.spark

import org.apache.spark.sql.{ Dataset, _ }
import zio.Task

final case class Write[T](
  ds: Dataset[T],
  options: Seq[(String, String)],
  format: Option[String],
  mode: Option[String]
) {
  def option(key: String, value: String): Write[T] = this.copy(options = options :+ (key -> value))

  def text(path: String): Task[Unit] =
    copy(format = Some("text")).save(path)

  def save(path: String): Task[Unit] =
    Task({
      type Write = DataFrameWriter[T]

      val w0: Write = ds.write
      val w1: Write = format.map(w0.format).getOrElse(w0)
      val w2: Write = mode.map(w1.mode).getOrElse(w1)

      w2.options(options.toMap).save(path)
    })

  def parquet(path: String): Task[Unit] =
    copy(format = Some("parquet")).save(path)

  def cache: Task[Unit] = Task(ds.cache)

  def format(name: String): Write[T] = copy(format = Some(name))

  def mode(writeMode: String): Write[T] = copy(mode = Some(writeMode))
}

object Write {
  def apply[T](ds: Dataset[T]): Write[T] = Write(ds, Nil, None, None)
}
