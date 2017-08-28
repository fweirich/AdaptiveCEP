package com.lambdarookie.eventscala.graph.nodes.traits

import akka.actor.{Actor, ActorRef}
import com.lambdarookie.eventscala.data.Queries._
import com.lambdarookie.eventscala.graph.monitors._
import com.lambdarookie.eventscala.backend.system.traits.{Operator, System}
import com.lambdarookie.eventscala.data.Events.{Created, Event}
import com.lambdarookie.eventscala.graph.factory.NodeFactory

trait Node extends Actor {

  val name: String = self.path.name

  val system: System
  val query: Query
  val operator: Operator
  val publishers: Map[String, ActorRef]
  val nodeData: NodeData
  val frequencyMonitor: AverageFrequencyMonitor
  val demandsMonitor: PathDemandsMonitor
  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  system.addNodeOperatorPair(self, operator)
  system.addQuery(query)

  def emitCreated(): Unit = {
    if (createdCallback.isDefined) createdCallback.get.apply() else context.parent ! Created
    frequencyMonitor.onCreated(nodeData)
    demandsMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (eventCallback.isDefined) eventCallback.get.apply(event) else context.parent ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    demandsMonitor.onEventEmit(event, nodeData)
  }

  def createChildNode(id: Int, query: Query, childOperator: Operator): ActorRef =
    NodeFactory.createNode(
      system, context, query, childOperator, publishers,
      AverageFrequencyMonitor(frequencyMonitor.interval, frequencyMonitor.logging, frequencyMonitor.testing),
      PathDemandsMonitor(demandsMonitor.messageInterval, demandsMonitor.latencyInterval,
        demandsMonitor.bandwidthInterval, demandsMonitor.throughputInterval, demandsMonitor.priority,
        demandsMonitor.logging),
      None, None, s"$name-$id-")

}
