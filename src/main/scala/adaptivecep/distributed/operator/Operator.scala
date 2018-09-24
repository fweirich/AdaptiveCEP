package adaptivecep.distributed.operator

import akka.actor.{ActorRef, Props}

trait Host

object NoHost extends Host

trait Operator {
  var host: Host
  val props: Props
  val dependencies: Seq[Operator]
  //val optimumPath: Map[Any, Operator]
}

case class ActiveOperator(host: Host, props: Props, /*tentativeOperators: Seq[Operator],
                          */dependencies: Seq[Operator]/*, optimumPath: Map[Any, Operator]*/) extends Operator
case class TentativeOperator(host: Host, props: Props, dependencies: Seq[Operator]/*, neighbor: Operator, optimumPath: Map[Any, Operator]*/) extends Operator
case class NodeHost(/*id: Int, */actorRef: ActorRef /*neighbors: Seq[Host]*/) extends Host