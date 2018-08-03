package adaptivecep.graph.nodes.traits

import java.util.concurrent.TimeUnit

import akka.actor.{ActorRef, Cancellable}
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.dsl.Dsl.stream
import adaptivecep.graph.qos._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}

trait UnaryNode extends Node {

  //override val query: UnaryQuery

  val createdCallback: Option[() => Any]
  val eventCallback: Option[(Event) => Any]

  //val childNode: ActorRef = createChildNode(1, query.sq)
  var childNode: ActorRef = self
  var parentNode: ActorRef = self
  val interval = 2
  var counter = 0

  var scheduledTask: Cancellable = _

  val query: Query1[Int] = stream[Int]("A")

  var frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  var latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor
  var nodeData: UnaryNodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)

  private val monitor: PathLatencyUnaryNodeMonitor = latencyMonitor.asInstanceOf[PathLatencyUnaryNodeMonitor]

  override def preStart(): Unit = {
    if(scheduledTask == null){
      scheduledTask = context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        if(!monitor.met && counter == 4) {
          controller ! RequirementsNotMet
          counter = 0
          //println(monitor.met)
        }
        else {
          counter += 1
        }
        if(monitor.latency.isDefined) {
          println(monitor.latency.get.toNanos/1000000.0)
        }
    })}}

  override def postStop(): Unit = {
    scheduledTask.cancel()
    monitor.scheduledTask.cancel()
  }

  def emitCreated(): Unit = {
    monitor.childNode = childNode
    if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    monitor.childNode = childNode
    if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
  }

  def delay(b: Boolean): Unit = {
    delay = b
    monitor.delay = true
  }

}
