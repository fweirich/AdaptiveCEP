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

  var scheduledTask: Cancellable = _

  val query: Query1[Int] = stream[Int]("A")

  var frequencyMonitor: UnaryNodeMonitor = frequencyMonitorFactory.createUnaryNodeMonitor
  var latencyMonitor: UnaryNodeMonitor = latencyMonitorFactory.createUnaryNodeMonitor
  var bandwidthMonitor: UnaryNodeMonitor = bandwidthMonitorFactory.createUnaryNodeMonitor
  var nodeData: UnaryNodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)

  val lmonitor: PathLatencyUnaryNodeMonitor = latencyMonitor.asInstanceOf[PathLatencyUnaryNodeMonitor]
  val bmonitor: PathBandwidthUnaryNodeMonitor = bandwidthMonitor.asInstanceOf[PathBandwidthUnaryNodeMonitor]

  var goodCounter: Int = 0
  var badCounter: Int = 0
  var failsafe: Int = 0

  var previousBandwidth : Double = 0
  var previousLatency : Duration = Duration.Zero

  override def preStart(): Unit = {
    if(scheduledTask == null){
      scheduledTask = context.system.scheduler.schedule(
      initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
      interval = FiniteDuration(interval, TimeUnit.SECONDS),
      runnable = () => {
        failsafe += 1
        if(failsafe > 2){
          failsafe = 0
          controller ! RequirementsNotMet
        }
        if(lmonitor.latency.isDefined && bmonitor.bandwidthForMonitoring.isDefined) {
          if(!lmonitor.met){
            goodCounter = 0
            badCounter += 1
            if(badCounter >= 3){
              badCounter = 0
              controller ! RequirementsNotMet
            }
          }
          if(lmonitor.met){
            goodCounter += 1
            badCounter = 0
            if(goodCounter >= 3){
              controller ! RequirementsMet
            }
          }
          println(lmonitor.latency.get.toNanos/1000000.0 + ", " + bmonitor.bandwidthForMonitoring.get)
          previousLatency = FiniteDuration(lmonitor.latency.get.toMillis, TimeUnit.MILLISECONDS)
          previousBandwidth = bmonitor.bandwidthForMonitoring.get
          lmonitor.latency = None
          bmonitor.bandwidthForMonitoring = None
        }
        else {
          println(previousLatency.toNanos/1000000.0 + ", " + previousBandwidth)
        }
    })}}

  override def postStop(): Unit = {
    scheduledTask.cancel()
    lmonitor.scheduledTask.cancel()
    //println("Shutting down....")
  }

  def emitCreated(): Unit = {
    lmonitor.childNode = childNode
    if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
    bandwidthMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    lmonitor.childNode = childNode
    if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
    frequencyMonitor.onEventEmit(event, nodeData)
    latencyMonitor.onEventEmit(event, nodeData)
    bandwidthMonitor.onEventEmit(event, nodeData)
  }

  def setDelay(b: Boolean): Unit = {
    delay = b
    //lmonitor.delay = b
  }

}
