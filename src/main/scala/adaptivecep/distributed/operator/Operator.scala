package adaptivecep.distributed.operator

import adaptivecep.data.Cost.Cost
import adaptivecep.data.Queries.Requirement
import akka.actor.{ActorRef, Props}
import rescala.default.Signal
import rescala.default.Event

import scala.concurrent.duration.Duration

trait Host

object NoHost extends Host

trait Operator {
  var host: Host
  val props: Props
  val dependencies: Seq[Operator]
}

trait  CEPSystem {
  val hosts: Signal[Set[Host]]
  val operators: Signal[Set[Operator]]
}

trait QoSSystem{
  val qos: Signal[Map[Host, Map[Host, Cost]]] //can be extracted from the query
  val demandViolated: Event[Requirement] // currently the node reports this via Requirements not met (could be changed to firing an event)
}

trait System extends CEPSystem with QoSSystem

case class ActiveOperator(var host: Host, props: Props, dependencies: Seq[Operator]) extends Operator
case class TentativeOperator(var host: Host, props: Props, dependencies: Seq[Operator]) extends Operator
case class NodeHost(actorRef: ActorRef) extends Host