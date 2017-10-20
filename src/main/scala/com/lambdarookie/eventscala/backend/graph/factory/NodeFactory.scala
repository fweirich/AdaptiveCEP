package com.lambdarookie.eventscala.backend.graph.factory

import akka.actor.{ActorRef, ActorRefFactory, Props}
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.backend.system.{BinaryOperator, EventSource, UnaryOperator}
import com.lambdarookie.eventscala.data.Events.Event
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.backend.graph.monitors.Monitor
import com.lambdarookie.eventscala.backend.graph.nodes._

/**
  * Created by monur.
  */
object NodeFactory {
  def createNode[T <: ActorRefFactory](system: System,
                                       actorRefFactory: T,
                                       query: Query,
                                       operator: Operator,
                                       publishers: Map[String, ActorRef],
                                       monitors: Set[_ <: Monitor],
                                       createdCallback: Option[() => Any],
                                       eventCallback: Option[(Event) => Any],
                                       prefix: String): ActorRef = query match {
    case streamQuery: StreamQuery =>
      actorRefFactory.actorOf(Props(
        StreamNode(
          system,
          streamQuery,
          operator.asInstanceOf[EventSource],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}stream")
    case sequenceQuery: SequenceQuery =>
      actorRefFactory.actorOf(Props(
        SequenceNode(
          system,
          sequenceQuery,
          operator.asInstanceOf[EventSource],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}sequence")
    case filterQuery: FilterQuery =>
      actorRefFactory.actorOf(Props(
        FilterNode(
          system,
          filterQuery,
          operator.asInstanceOf[UnaryOperator],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}filter")
    case dropElemQuery: DropElemQuery =>
      actorRefFactory.actorOf(Props(
        DropElemNode(
          system,
          dropElemQuery,
          operator.asInstanceOf[UnaryOperator],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}dropelem")
    case selfJoinQuery: SelfJoinQuery =>
      actorRefFactory.actorOf(Props(
        SelfJoinNode(
          system,
          selfJoinQuery,
          operator.asInstanceOf[UnaryOperator],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}selfjoin")
    case joinQuery: JoinQuery =>
      actorRefFactory.actorOf(Props(
        JoinNode(
          system,
          joinQuery,
          operator.asInstanceOf[BinaryOperator],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}join")
    case conjunctionQuery: ConjunctionQuery =>
      actorRefFactory.actorOf(Props(
        ConjunctionNode(
          system,
          conjunctionQuery,
          operator.asInstanceOf[BinaryOperator],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}conjunction")
    case disjunctionQuery: DisjunctionQuery =>
      actorRefFactory.actorOf(Props(
        DisjunctionNode(
          system,
          disjunctionQuery,
          operator.asInstanceOf[BinaryOperator],
          publishers,
          monitors,
          createdCallback,
          eventCallback)),
        s"${prefix}disjunction")
  }
}