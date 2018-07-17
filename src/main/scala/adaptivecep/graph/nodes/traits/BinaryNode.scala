package adaptivecep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._
import akka.actor.ActorRef

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

trait BinaryNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  val query: Query1[Int] = stream[Int]("A")

  val interval = 5

  var childNode1: ActorRef = self
  var childNode2: ActorRef = self

  var parentNode: ActorRef = self

  var frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  var latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  var nodeData: BinaryNodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)

  override def preStart(): Unit = {context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      if(!latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor].met){
        controller ! RequirementsNotMet
        println(latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor].met)
      }
    })}

  def emitCreated(): Unit = {
    frequencyMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
    latencyMonitor = latencyMonitorFactory.createBinaryNodeMonitor
    if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

}