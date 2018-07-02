package adaptivecep.graph.nodes.traits

import adaptivecep.data.Events._
import adaptivecep.data.Queries.Query1
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._
import akka.actor.ActorRef

trait LeafNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  var parentNode: ActorRef = self

  val query: Query1[Int] = stream[Int]("A")

  val frequencyMonitor: LeafNodeMonitor = frequencyMonitorFactory.createLeafNodeMonitor
  val latencyMonitor: LeafNodeMonitor = latencyMonitorFactory.createLeafNodeMonitor
  val nodeData: LeafNodeData = LeafNodeData(name, requirements, context)

  def emitCreated(): Unit = {
    if (createdCallback.isDefined) createdCallback.get.apply() else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

}
