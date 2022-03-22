package zio.spark.experimental

import zio._
import zio.spark.sql.{Dataset, SIO, SparkSession}

/**
 * A class representing a Spark pipeline. Generally speaking, a Spark
 * pipeline can be divided in three components:
 *   - We load a dataframe from an external source => The load effect
 *   - We transform this dataframe using Spark => The transform function
 *   - We do something with the output => The action effect
 *
 * @param load
 *   The effect to create an dataset in input
 * @param transform
 *   The whole dataset processing
 * @param action
 *   The effect to extract a result from the dataset transformation
 * @tparam Source
 *   The input type of the dataset
 * @tparam Output
 *   The output type of the dataset
 * @tparam Result
 *   The result type of the pipeline
 */
final case class Pipeline[Source, Output, Result](
    load:      SIO[Dataset[Source]],
    transform: Dataset[Source] => Dataset[Output],
    action:    Dataset[Output] => Task[Result]
) {

  /**
   * Runs the pipeline computation as a ZIO effect. You must provide a
   * [[SparkSession]] layer to actually run the effect.
   */
  def run: SIO[Result] = load.map(transform).flatMap(action)
}

object Pipeline {

  /** Builds a pipeline without processing. */
  def buildWithoutTransformation[Source, Result](load: SIO[Dataset[Source]])(
      action: Dataset[Source] => Task[Result]
  ): Pipeline[Source, Source, Result] = build(load)(df => df)(action)

  /**
   * Builds a pipeline using type inference, you can't use Pipeline case
   * class constructor without specifying each function types since
   * Scala is not capable to find the correct types by itself.
   */
  def build[Source, Output, Result](load: SIO[Dataset[Source]])(
      transform: Dataset[Source] => Dataset[Output]
  )(action: Dataset[Output] => Task[Result]): Pipeline[Source, Output, Result] =
    Pipeline(
      load      = load,
      transform = transform,
      action    = action
    )
}
