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

  val interval = 3
  var badCounter = 0
  var goodCounter = 0
  var failsafe = 0

  var childNode1: ActorRef = self
  var childNode2: ActorRef = self

  var parentNode: ActorRef = self

  var frequencyMonitor: BinaryNodeMonitor = frequencyMonitorFactory.createBinaryNodeMonitor
  var latencyMonitor: BinaryNodeMonitor = latencyMonitorFactory.createBinaryNodeMonitor
  var bandwidthMonitor: BinaryNodeMonitor = bandwidthMonitorFactory.createBinaryNodeMonitor
  var nodeData: BinaryNodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
  var scheduledTask: Cancellable = _
  var resetTask: Cancellable = _

  val lmonitor: PathLatencyBinaryNodeMonitor = latencyMonitor.asInstanceOf[PathLatencyBinaryNodeMonitor]
  var fMonitor: AverageFrequencyBinaryNodeMonitor = frequencyMonitor.asInstanceOf[AverageFrequencyBinaryNodeMonitor]
  //val bmonitor: PathBandwidthBinaryNodeMonitor = bandwidthMonitor.asInstanceOf[PathBandwidthBinaryNodeMonitor]

  var previousBandwidth : Double = 0
  var previousLatency : Duration = Duration.Zero

  override def preStart(): Unit = {
    if(scheduledTask == null){
      scheduledTask = context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          if (lmonitor.latency.isDefined && fMonitor.averageOutput.isDefined /*bmonitor.bandwidthForMonitoring.isDefined*/ ) {
            if (!lmonitor.met || !fMonitor.met) {
              lmonitor.met = true
              //bmonitor.met = true
              fMonitor.met = true
              controller ! RequirementsNotMet
            }

            if (lmonitor.met && fMonitor.met) {
              controller ! RequirementsMet
            }
            println(lmonitor.latency.get.toNanos / 1000000.0 + ", " + fMonitor.averageOutput.get /*bmonitor.bandwidthForMonitoring.get*/)
            previousLatency = FiniteDuration(lmonitor.latency.get.toMillis, TimeUnit.MILLISECONDS)
            previousBandwidth = fMonitor.averageOutput.get
            lmonitor.latency = None
            fMonitor.averageOutput = None
            //bmonitor.bandwidthForMonitoring = None
          } else {
            println(previousLatency.toNanos/1000000.0 + ", " + previousBandwidth)
          }
        }
      )
    }
  }

  override def postStop(): Unit = {
    scheduledTask.cancel()
    lmonitor.scheduledTask.cancel()
    //bmonitor.scheduledTask.cancel()
    println("Shutting down....")
  }

  def emitCreated(): Unit = {
    if(resetTask != null){
      resetTask = context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(1000, TimeUnit.MILLISECONDS),
        runnable = () => {
          emittedEvents = 0
        })
    }
    lmonitor.childNode1 = childNode1
    lmonitor.childNode2 = childNode2
    //bmonitor.childNode1 = childNode1
    //bmonitor.childNode2 = childNode2
    if (createdCallback.isDefined) createdCallback.get.apply() //else parentNode ! Created
    frequencyMonitor.onCreated(nodeData)
    latencyMonitor.onCreated(nodeData)
    bandwidthMonitor.onCreated(nodeData)
  }

  def emitEvent(event: Event): Unit = {
    context.system.scheduler.scheduleOnce(
      FiniteDuration(costs(parentNode).duration.toMillis, TimeUnit.MILLISECONDS),
      () => {
        println(emittedEvents)
        println(costs(parentNode))
        lmonitor.childNode1 = childNode1
        lmonitor.childNode2 = childNode2
        if(parentNode == self || (parentNode != self && emittedEvents < costs(parentNode).bandwidth.toInt)) {
          emittedEvents += 1
          //bmonitor.childNode1 = childNode1
          //bmonitor.childNode2 = childNode2
          if (eventCallback.isDefined) eventCallback.get.apply(event) else parentNode ! event
          frequencyMonitor.onEventEmit(event, nodeData)
          latencyMonitor.onEventEmit(event, nodeData)
          bandwidthMonitor.onEventEmit(event, nodeData)
        }
      }
    )
  }
}