package adaptivecep.graph.nodes.traits

import akka.actor.ActorRef
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._

trait UnaryNode extends Node {

  //override val query: UnaryQuery

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  //val childNode: ActorRef = createChildNode(1, query.sq)
  var childNode: ActorRef = self
  var parentNode: ActorRef = self

  val query: Query1[Int] = stream[Int]("A")

  val frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  val latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor
  var nodeData: UnaryNodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)

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
