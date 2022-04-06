package example

import SimpleApp.{job => simpleAppExample}
import SparkCodeMigration.{job => sparkCodeMigrationExample}
import WordCount.{job => wordCountExample}
import org.apache.log4j.{Level, Logger}

import zio._
import zio.cli.{Args, CliApp, Command, Options}
import zio.cli.HelpDoc.Span.text
import zio.spark.parameter._
import zio.spark.sql._

import java.util.concurrent.TimeUnit

object ZIOEcosystem extends ZIOAppDefault {
  Logger.getLogger("org").setLevel(Level.OFF) // Disable Spark logging

  // A more sophisticated layer to add middleware logs
  private val session: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      for {
        _ <- ZIO.logInfo("Opening Spark Session...")
        builder = SparkSession.builder.master(localAllNodes).appName("zio-environment")
        session <-
          ZIO.acquireRelease(builder.getOrCreate) { ss =>
            for {
              _ <- ZIO.logInfo("Closing Spark Session...")
              _ <-
                Task
                  .attempt(ss.close)
                  .foldZIO(
                    failure = _ => ZIO.logError("Failed to close the Spark Session."),
                    success = _ => ZIO.unit
                  )
            } yield ()
          }
      } yield session
    }

  sealed trait Example { self =>
    val name: String
    def toArgs: (String, Example) = (name, self)
  }

  case object SimpleApp extends Example {
    val name = "simple-app"
  }
  case object WordCount extends Example {
    val name = "word-count"
  }
  case object SparkCodeMigration extends Example {
    val name = "spark-code-migration"
  }
  case object All extends Example {
    val name = "all"
  }

  case class RunSubcommand(example: Example)

  val runSubcommandArgs: Args[Example] =
    Args.enumeration(
      SimpleApp.toArgs,
      WordCount.toArgs,
      SparkCodeMigration.toArgs,
      All.toArgs
    )

  val exampleSubcommand: Command[RunSubcommand] = Command("example", Options.none, runSubcommandArgs).map(RunSubcommand)
  val zioSparkCommand: Command[RunSubcommand] =
    Command("zio-spark", Options.none, Args.none).subcommands(exampleSubcommand)

  val app: CliApp[SparkSession, Throwable, RunSubcommand] =
    CliApp.make(
      "ZIO Spark Application",
      "0.1.0",
      text("An example to show that ZIO Spark works with the ZIO ecosystem."),
      zioSparkCommand
    )(program)

  def program(command: RunSubcommand): SIO[Any] =
    for {
      _ <-
        command.example match {
          case All => ZIO.logInfo("You selected 'all', running all the available examples...")
          case _   => ZIO.logInfo(s"You selected '${command.example.name}', running this example...")
        }
      clock  <- ZIO.clock
      before <- clock.currentTime(TimeUnit.MICROSECONDS)
      _ <-
        command.example match {
          case SimpleApp          => simpleAppExample
          case WordCount          => wordCountExample
          case SparkCodeMigration => sparkCodeMigrationExample
          case All => ZIO.collectAllParDiscard(List(simpleAppExample, wordCountExample, sparkCodeMigrationExample))
        }
      after <- clock.currentTime(TimeUnit.MICROSECONDS)
      duration = (after - before).toFloat / 1_000_000f
      _ <- ZIO.logInfo(s"Example(s) correctly finished, it took $duration seconds!")
    } yield ()

  // zio-spark run simple-app => Run the simple-app example
  // zio-spark run all        => Run all examples in parallel
  override def run: ZIO[ZIOAppArgs with Scope, Nothing, Unit] =
    for {
      // args <- ZIOAppArgs.getArgs // We comment this line for the example, we don't use arguments directly.
      _ <- app.run(List("example", "all")).provide(session).orDie
    } yield ()
}
