package adaptivecep.distributed.operator

import akka.actor.{ActorRef, Props}

trait Host

object NoHost extends Host

trait Operator {
  var host: Host
  val props: Props
  val dependencies: Seq[Operator]
}

case class ActiveOperator(var host: Host, props: Props, dependencies: Seq[Operator]) extends Operator
case class TentativeOperator(var host: Host, props: Props, dependencies: Seq[Operator]) extends Operator
case class NodeHost(actorRef: ActorRef) extends Host