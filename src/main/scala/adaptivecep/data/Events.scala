package adaptivecep.data

import java.time.Instant

import adaptivecep.data.Cost._
import adaptivecep.distributed.operator.{ActiveOperator, TentativeOperator}
import akka.actor.{ActorRef, Props}
import akka.dispatch.ControlMessage

import scala.concurrent.duration.Duration

object Events {

  case object Created

  trait CEPControlMessage extends ControlMessage

  sealed trait PlacementEvent extends ControlMessage

  case object RequirementsMet extends PlacementEvent
  case object RequirementsNotMet extends PlacementEvent


  //Tentative Operator Phase
  case class CostMessage(latency: Duration, bandwidth: Double) extends PlacementEvent

  //Migration Phase
  case object MigrationComplete extends PlacementEvent


  case class BecomeActiveOperator(operator: ActiveOperator) extends PlacementEvent
  case class SetActiveOperator(operator: Props) extends PlacementEvent

  case class BecomeTentativeOperator(operator: TentativeOperator, parentNode: ActorRef,
                                     parentHosts: Seq[ActorRef], childHost1: Option[ActorRef],
                                     childHost2: Option[ActorRef], temperature: Double) extends PlacementEvent

  case class ChooseTentativeOperators(tentativeParents: Seq[ActorRef]) extends PlacementEvent

  case object OperatorRequest extends PlacementEvent
  case class OperatorResponse(active: Option[ActiveOperator], tentative: Option[TentativeOperator]) extends PlacementEvent

  case class ParentResponse(parent: Option[ActorRef]) extends PlacementEvent

  case class ChildHost1(actorRef: ActorRef) extends PlacementEvent
  case class ChildHost2(actorRef1: ActorRef, actorRef2: ActorRef) extends PlacementEvent
  case class ChildResponse(childNode: ActorRef) extends PlacementEvent

  case class ParentHost(parentHost: ActorRef, parentNode: ActorRef) extends PlacementEvent
  case class FinishedChoosing(tentativeChildren: Seq[ActorRef]) extends  PlacementEvent

  case object Start extends PlacementEvent

  case class CostRequest(instant: Instant) extends PlacementEvent
  case class CostResponse(instant: Instant, bandwidth: Double) extends PlacementEvent
  case class LatencyCostResponse(instant: Instant) extends PlacementEvent
  case class BandwidthCostResponse(bandwidth: Double) extends PlacementEvent

  case class StateTransferMessage(optimumHosts: Seq[ActorRef], parentNode: ActorRef) extends PlacementEvent
  case object TentativeAcknowledgement extends PlacementEvent
  case object ContinueSearching extends PlacementEvent

  case object ResetTemperature extends PlacementEvent

  case object CentralizedCreated

  case class StartThroughPutMeasurement(instant: Instant) extends PlacementEvent
  case class EndThroughPutMeasurement(instant: Instant, actual: Int) extends PlacementEvent
  case object TestEvent extends PlacementEvent

  case object InitializeQuery extends CEPControlMessage
  case class Delay(delay: Boolean)

  case object AllHosts extends CEPControlMessage
  case class Hosts(h: Set[ActorRef]) extends CEPControlMessage

  case class HostToNodeMap(m: Map[ActorRef, ActorRef]) extends CEPControlMessage

  case class Node(actorRef: ActorRef) extends CEPControlMessage

  case class Neighbors(neighbors: Set[ActorRef], allHosts: Set[ActorRef]) extends CEPControlMessage

  case class Controller(controller: ActorRef) extends CEPControlMessage
  case class OptimizeFor(optimizer: String) extends CEPControlMessage

  sealed trait Child extends CEPControlMessage
  case class Child1(c1: ActorRef)               extends Child with CEPControlMessage
  case class Child2(c1: ActorRef, c2: ActorRef) extends Child with CEPControlMessage

  case class ChildUpdate(old: ActorRef, newChild: ActorRef) extends CEPControlMessage

  case class Parent(p1: ActorRef) extends CEPControlMessage

  case object KillMe extends CEPControlMessage
  case object Kill extends CEPControlMessage

  case class LatencyRequest(instant: Instant) extends CEPControlMessage
  case class LatencyResponse(instant: Instant) extends CEPControlMessage
  case class ThroughPutResponse(eventsPerSecond: Int) extends CEPControlMessage
  case object HostPropsRequest extends CEPControlMessage
  case class HostPropsResponse(latencies: Map[ActorRef, Cost]) extends CEPControlMessage

  case object DependenciesRequest extends CEPControlMessage
  case class DependenciesResponse(dependencies: Seq[ActorRef]) extends CEPControlMessage

  sealed trait Event
  case class Event1(e1: Any)                                              extends Event
  case class Event2(e1: Any, e2: Any)                                     extends Event
  case class Event3(e1: Any, e2: Any, e3: Any)                            extends Event
  case class Event4(e1: Any, e2: Any, e3: Any, e4: Any)                   extends Event
  case class Event5(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any)          extends Event
  case class Event6(e1: Any, e2: Any, e3: Any, e4: Any, e5: Any, e6: Any) extends Event

  //val errorMsg: String = "Panic! Control flow should never reach this point!"

  def toFunEventAny[A](f: (A) => Any): Event => Any = {
    case Event1(e1) => f.asInstanceOf[(Any) => Any](e1)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B](f: (A, B) => Any): Event => Any = {
    case Event2(e1, e2) => f.asInstanceOf[(Any, Any) => Any](e1, e2)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C](f: (A, B, C) => Any): Event => Any = {
    case Event3(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Any](e1, e2, e3)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D](f: (A, B, C, D) => Any): Event => Any = {
    case Event4(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Any](e1, e2, e3, e4)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D, E](f: (A, B, C, D, E) => Any): Event => Any = {
    case Event5(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventAny[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Any): Event => Any = {
    case Event6(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Any](e1, e2, e3, e4, e5, e6)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A](f: (A) => Boolean): Event => Boolean = {
    case Event1(e1) => f.asInstanceOf[(Any) => Boolean](e1)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B](f: (A, B) => Boolean): Event => Boolean = {
    case Event2(e1, e2) => f.asInstanceOf[(Any, Any) => Boolean](e1, e2)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C](f: (A, B, C) => Boolean): Event => Boolean = {
    case Event3(e1, e2, e3) => f.asInstanceOf[(Any, Any, Any) => Boolean](e1, e2, e3)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D](f: (A, B, C, D) => Boolean): Event => Boolean = {
    case Event4(e1, e2, e3, e4) => f.asInstanceOf[(Any, Any, Any, Any) => Boolean](e1, e2, e3, e4)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D, E](f: (A, B, C, D, E) => Boolean): Event => Boolean = {
    case Event5(e1, e2, e3, e4, e5) => f.asInstanceOf[(Any, Any, Any, Any, Any) => Boolean](e1, e2, e3, e4, e5)
    //case _ => sys.error(errorMsg)
  }

  def toFunEventBoolean[A, B, C, D, E, F](f: (A, B, C, D, E, F) => Boolean): Event => Boolean = {
    case Event6(e1, e2, e3, e4, e5, e6) => f.asInstanceOf[(Any, Any, Any, Any, Any, Any) => Boolean](e1, e2, e3, e4, e5, e6)
    //case _ => sys.error(errorMsg)
  }

}
