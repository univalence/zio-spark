package zio.spark.codegen.generation

import zio.{Accessible, Console, UIO, ULayer, URIO, URLayer, ZIO, ZLayer}

trait Logger {
  def info(text: => String): UIO[Unit]
  def success(text: => String): UIO[Unit]
  def error(text: => String): UIO[Unit]
}

object Logger extends Accessible[Logger] {
  def info(text: => String): URIO[Logger, Unit]    = Logger(_.info(text))
  def success(text: => String): URIO[Logger, Unit] = Logger(_.success(text))
  def error(text: => String): URIO[Logger, Unit]   = Logger(_.error(text))

  val live: URLayer[Console, Logger] =
    ZLayer {
      for {
        console <- ZIO.service[Console]
      } yield LoggerLive(console)
    }

  val silent: ULayer[Logger] = ZLayer.succeed(LoggerSilent)

  case class LoggerLive(console: Console) extends Logger {

    /** Color a text to red in the terminal. */
    private def red(text: String): String = "\u001B[31m" + text + "\u001B[0m"

    /** Color a text to red in the terminal. */
    private def green(text: String): String = "\u001b[32m" + text + "\u001B[0m"

    private def log(status: String, text: => String)(transformer: String => String): UIO[Unit] =
      ZIO.foreachDiscard(text.split("\n"))(line => console.printLine(s"[${transformer(status)}] $line")).orDie

    def info(text: => String): UIO[Unit] = log("info", text)(identity)

    def success(text: => String): UIO[Unit] = log("success", text)(green)

    def error(text: => String): UIO[Unit] = log("error", text)(red)
  }

  case object LoggerSilent extends Logger {
    override def info(text: => String): UIO[Unit] = ZIO.unit

    override def success(text: => String): UIO[Unit] = ZIO.unit

    override def error(text: => String): UIO[Unit] = ZIO.unit
  }
}
