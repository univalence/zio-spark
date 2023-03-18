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

object ZIOEcosystem extends ZIOAppDefault {
  // A more sophisticated layer to add middleware logs
  private val session: ZLayer[Any, Throwable, SparkSession] =
    ZLayer.scoped {
      val disableSparkLogging: UIO[Unit] = ZIO.succeed(Logger.getLogger("org").setLevel(Level.OFF))

      val builder: SparkSession.Builder = SparkSession.builder.master(localAllNodes).appName("zio-ecosystem")

      val build: ZIO[Scope, Throwable, SparkSession] =
        builder.getOrCreate.withFinalizer { ss =>
          ZIO.logInfo("Closing Spark Session ...") *>
            ss.close.tapError(_ => ZIO.logError("Failed to close the Spark Session.")).orDie
        }

      ZIO.logInfo("Opening Spark Session...") *> disableSparkLogging *> build
    }

  sealed abstract class Example {
    final def name: String =
      this match {
        case Example.SimpleApp          => "simple-app"
        case Example.WordCount          => "word-count"
        case Example.SparkCodeMigration => "spark-code-migration"
        case Example.All                => "all"
      }

  }

  object Example {
    case object SimpleApp extends Example

    case object WordCount extends Example

    case object SparkCodeMigration extends Example

    case object All extends Example

    val values: Seq[Example] = Seq(SimpleApp, WordCount, SparkCodeMigration, All)

  }

  import Example._

  final case class RunSubcommand(example: Example)

  val runSubcommandArgs: Args[Example] = Args.enumeration(Example.values.map(x => (x.name, x)): _*)

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

  def program(command: RunSubcommand): SIO[Any] = {

    def logInfo(example: Example): UIO[Unit] =
      example match {
        case All => ZIO.logInfo("You selected 'all', running all the available examples...")
        case _   => ZIO.logInfo(s"You selected '${example.name}', running this example...")
      }

    def job(example: Example): SIO[Unit] =
      example match {
        case SimpleApp          => simpleAppExample
        case WordCount          => wordCountExample
        case SparkCodeMigration => sparkCodeMigrationExample
        case All                => ZIO.collectAllParDiscard(Example.values.filter(_ != All).map(job))
      }

    // operator style
    logInfo(command.example) *> job(command.example).timed.tap { case (duration, _) =>
      val seconds: Float = duration.toMillis.toFloat / 1000
      ZIO.logInfo(s"Example (${command.example.name}) correctly finished, it took $seconds seconds!")
    }

    // for-comprehension style
    for {
      _   <- logInfo(command.example)
      run <- job(command.example).timed
      seconds: Float = run._1.toMillis.toFloat / 1000
      _ <- ZIO.logInfo(s"Example (${command.example.name}) correctly finished, it took $seconds seconds!")
    } yield {}
  }

  // zio-spark run simple-app => Run the simple-app example
  // zio-spark run all        => Run all examples in parallel
  override def run: URIO[ZIOAppArgs with Scope, Unit] =
    for {
      args <- ZIOAppArgs.getArgs
      _    <- Console.printLine("no args supplied, running all examples (`example all`)").when(args.isEmpty).ignore
      _    <- app.run(if (args.isEmpty) List("example", "all") else args.toList).provide(session).logError.ignore
    } yield ()
}
