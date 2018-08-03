package adaptivecep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, Cancellable}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}

trait BinaryNode extends Node {

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  val query: Query1[Int] = stream[Int]("A")

  val interval = 2

  var childNode1: ActorRef = self
  var childNode2: ActorRef = self

  var parentNode: ActorRef = self

  var frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  var latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  var nodeData: BinaryNodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
  var scheduledTask: Cancellable = _

  private val monitor: PathLatencyBinaryNodeMonitor = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor]

  override def preStart(): Unit = {
    if(scheduledTask == null){
      scheduledTask = context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        if(!monitor.met){
          controller ! RequirementsNotMet
          //println(monitor.met)
        }
        //val pathLatency1 = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor].childNode1PathLatency
        //val pathLatency2 = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor].childNode2PathLatency
        if(monitor.latency.isDefined) {
          println(monitor.latency.get.toNanos/1000000.0)
        }
      }
  )}}

  override def postStop(): Unit = {
    scheduledTask.cancel()
    monitor.scheduledTask.cancel()
  }

  def emitCreated(): Unit = {
    monitor.childNode1 = childNode1
    monitor.childNode2 = childNode2
    if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    monitor.childNode1 = childNode1
    monitor.childNode2 = childNode2
    if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

  def setDelay(b: Boolean): Unit = {
    delay = b
    monitor.delay = true
  }

}