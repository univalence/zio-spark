package zio.spark.experimental

import zio.{Ref, Trace, UIO, ZIO}
import zio.prelude.Assertion._
import zio.prelude.Subtype
import zio.spark.experimental.NewType._

object NewType {
  object Weight extends Subtype[Long] { self =>
    @SuppressWarnings(Array("scalafix:ExplicitResultTypes"))
    override def assertion =
      assert {
        greaterThan(0L)
      }

    def unsafeMake(n: Long): Weight = Weight.wrap(n)
  }

  object Ratio extends Subtype[Double] {
    @SuppressWarnings(Array("scalafix:ExplicitResultTypes"))
    override def assertion =
      assert {
        greaterThanOrEqualTo(0.0) && lessThanOrEqualTo(1.0)
      }

    def unsafeMake(n: Double): Ratio = Ratio.wrap(n)

    implicit val ordering: Ordering[Ratio] = Ordering.by(Ratio.unwrap)

    val zero: Ratio = Ratio(0.0)
    val full: Ratio = Ratio(1.0)
    val p05: Ratio  = Ratio(0.05)

    def mean(w1: Weight, r1: Ratio)(w2: Weight, r2: Ratio): Ratio = {
      val weightedRatio1 = Ratio.unwrap(r1) * Weight.unwrap(w1)
      val weightedRatio2 = Ratio.unwrap(r2) * Weight.unwrap(w2)
      val sumWeight      = Weight.unwrap(w1) + Weight.unwrap(w2)
      Ratio.wrap((weightedRatio1 + weightedRatio2) / sumWeight)
    }
  }

  type Weight = Weight.Type
  type Ratio  = Ratio.Type
}

/**
 * A `Tap` adjusts the flow of tasks through an external service in
 * response to observed failures in the service, always trying to
 * maximize flow while attempting to meet the user-defined upper bound
 * on failures.
 */
trait CircuitTap[-E1, +E2] {

  /**
   * Sends the task through the tap. The returned task may fail
   * immediately with a default error depending on the service being
   * guarded by the tap.
   */
  def apply[R, E >: E2 <: E1, A](effect: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A]

  def getState(implicit trace: Trace): UIO[CircuitTap.State]
}

class SmartCircuitTap[-E1, +E2](
    errBound:  Ratio,
    qualified: E1 => Boolean,
    rejected:  => E2,
    state:     Ref[CircuitTap.State]
) extends CircuitTap[E1, E2] {
  override def apply[R, E >: E2 <: E1, A](effect: ZIO[R, E, A])(implicit trace: Trace): ZIO[R, E, A] =
    state.get flatMap { s =>
      val tooMuchError: Boolean = s.decayingErrorRatio.ratio > errBound
      if (tooMuchError) {
        state.update(_.incRejected) *> ZIO.fail(rejected)
      } else {
        run(effect)
      }
    }

  private def run[R, E >: E2 <: E1, A](effect: ZIO[R, E, A]): ZIO[R, E, A] = {
    def tapError(e: E): UIO[Unit] =
      if (qualified(e)) state.update(_.incFailure)
      else state.update(_.incSuccess)

    effect.tapBoth(tapError, _ => state.update(_.incSuccess))
  }

  override def getState(implicit trace: Trace): UIO[CircuitTap.State] = state.get
}

final case class DecayingRatio(ratio: Ratio, scale: Weight) {
  def decay(value: Ratio, maxScale: Weight): DecayingRatio =
    DecayingRatio(Ratio.mean(scale, ratio)(Weight(1L), value), if (scale < maxScale) scale else maxScale)
}

object CircuitTap {
  final private[spark] case class State(
      failed:             Long,
      success:            Long,
      rejected:           Long,
      decayingErrorRatio: DecayingRatio
  ) {

    def nextErrorRatio(ratio: Ratio): DecayingRatio =
      if (total == 0) DecayingRatio(ratio, decayingErrorRatio.scale)
      else decayingErrorRatio.decay(ratio, Weight.unsafeMake(total))

    def updateRatio(ratio: Ratio): State = copy(decayingErrorRatio = nextErrorRatio(ratio))

    def total: Long = failed + success + rejected

    def incFailure: State = updateRatio(Ratio.full).copy(failed = failed + 1)

    def incSuccess: State = updateRatio(Ratio.zero).copy(success = success + 1)

    def incRejected: State = updateRatio(Ratio.zero).copy(rejected = rejected + 1)

    def totalErrorRatio: Ratio = if (total == 0) Ratio.zero else Ratio.unsafeMake(failed.toDouble / total)
  }

  private def zeroState(scale: Weight): State = State(0, 0, 0, DecayingRatio(Ratio.zero, scale))

  // TODO: ChangeMaker with configure
  /* make[RejectionError](maxError: Ratio, decayScale:Int, rejected: CircuitTap.State =>
   * RejectionError).qualifyErrors[E](qualifie: E => Boolean) */
  // .manage(effect)
  // .mamageAndQualify(effect)(qualifie)
  /**
   * Creates a tap that aims for the specified maximum error rate, using
   * the specified function to qualify errors (unqualified errors are
   * not treated as failures for purposes of the tap), and the specified
   * default error used for rejecting tasks submitted to the tap.
   */
  def make[E1, E2](
      maxError: Ratio,
      qualified: E1 => Boolean,
      rejected: => E2,
      decayScale: Weight
  )(implicit trace: Trace): UIO[CircuitTap[E1, E2]] =
    for {
      state <- Ref.make(zeroState(decayScale))
    } yield new SmartCircuitTap[E1, E2](maxError, qualified, rejected, state)
}
