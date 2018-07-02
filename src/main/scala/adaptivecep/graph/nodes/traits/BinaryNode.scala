package adaptivecep.graph.nodes.traits

import akka.actor.ActorRef
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.{frequency, latency, nStream, ratio, sequence, slidingWindow, stream, timespan}
import adaptivecep.graph.qos._

trait BinaryNode extends Node {

  //override val query: BinaryQuery

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  //val childNode1: ActorRef = createChildNode(1, query.sq1)
  //val childNode2: ActorRef = createChildNode(2, query.sq2)

  val query: Query1[Int] = stream[Int]("A")


  var childNode1: ActorRef = self
  var childNode2: ActorRef = self

  var parentNode: ActorRef = self

  val frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  val latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  var nodeData: BinaryNodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2)

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