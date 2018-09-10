package adaptivecep.data

import java.time.Instant

import adaptivecep.distributed.{ActiveOperator, TentativeOperator}
import akka.actor.{ActorRef, Props}

import scala.concurrent.duration.Duration

object Events {

  case object Created

  sealed trait GreedyPlacementEvent
  case class CostMessage(latency: Duration) extends GreedyPlacementEvent
  case class BecomeActiveOperator(operator: ActiveOperator) extends GreedyPlacementEvent
  case class SetActiveOperator(operator: Props) extends GreedyPlacementEvent
  case class BecomeTentativeOperator(operator: TentativeOperator, parentNode: ActorRef,
                                     parentHosts: Seq[ActorRef], childHost1: Option[ActorRef],
                                     childHost2: Option[ActorRef]) extends GreedyPlacementEvent
  case class BecomeTentativeOperatorWithTemperature(operator: TentativeOperator, parentNode: ActorRef,
                                     parentHosts: Seq[ActorRef], childHost1: Option[ActorRef],
                                     childHost2: Option[ActorRef], temperature: Double) extends GreedyPlacementEvent
  case class ChooseTentativeOperators(tentativeParents: Seq[ActorRef]) extends GreedyPlacementEvent
  case object OperatorRequest extends GreedyPlacementEvent
  case class OperatorResponse(active: Option[ActiveOperator], tentative: Option[TentativeOperator]) extends GreedyPlacementEvent
  case class ParentResponse(parent: Option[ActorRef]) extends GreedyPlacementEvent
  case class ChildHost1(actorRef: ActorRef) extends GreedyPlacementEvent
  case class ChildHost2(actorRef1: ActorRef, actorRef2: ActorRef) extends GreedyPlacementEvent
  case class ChildResponse(childNode: ActorRef) extends GreedyPlacementEvent
  case class ParentHost(parentHost: ActorRef, parentNode: ActorRef) extends GreedyPlacementEvent
  case class FinishedChoosing(tentativeChildren: Seq[ActorRef]) extends  GreedyPlacementEvent
  case object Start extends GreedyPlacementEvent
  case class CostRequest(instant: Instant) extends GreedyPlacementEvent
  case class CostResponse(instant: Instant) extends GreedyPlacementEvent
  case class StateTransferMessage(optimumHosts: Seq[ActorRef], parentNode: ActorRef) extends GreedyPlacementEvent
  case object MigrationComplete extends GreedyPlacementEvent

  case object CentralizedCreated

  case object InitializeQuery
  case class Delay(delay: Boolean)

  case object AllHosts
  case class Hosts(h: Set[ActorRef])

  case class Node(actorRef: ActorRef)

  case class Neighbors(neighbors: Set[ActorRef])

  case class Controller(controller: ActorRef)

  sealed trait Child
  case class Child1(c1: ActorRef)               extends Child
  case class Child2(c1: ActorRef, c2: ActorRef) extends Child

  case class ChildUpdate(old: ActorRef, newChild: ActorRef)

  case class Parent(p1: ActorRef)

  case class Move(a: ActorRef)

  case object KillMe
  case object Kill

  case object RequirementsNotMet extends GreedyPlacementEvent
  case object RequirementsMet extends GreedyPlacementEvent

  case class LatencyRequest(instant: Instant)
  case class LatencyResponse(instant: Instant)
  case object HostPropsRequest
  case class HostPropsResponse(latencies: Map[ActorRef, Duration])

  case object DependenciesRequest
  case class DependenciesResponse(dependencies: Seq[ActorRef])

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
