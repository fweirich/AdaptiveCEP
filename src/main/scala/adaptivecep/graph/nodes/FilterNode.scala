package adaptivecep.graph.nodes

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.qos._
import akka.actor.{ActorRef, PoisonPill}

case class FilterNode(
    //query: FilterQuery,
    requirements: Set[Requirement],
    cond: Event => Boolean,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends UnaryNode {

  def moveTo(a: ActorRef): Unit = {
    a ! Parent(parentNode)
    a ! Child1(childNode)
    childNode ! Parent(a)
    parentNode ! ChildUpdate(self, a)
    childNode ! KillMe
  }
  var parentReceived: Boolean = false
  var childCreated: Boolean = false

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      childCreated = true
      //if (parentReceived && !created) emitCreated()
    case event: Event if sender() == childNode => {
      if (cond(event)) emitEvent(event)
    }
    case CentralizedCreated =>
      if(!created){
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
      //if (childCreated && !created) emitCreated()
    }
    case Child1(c) => {
      emitCreated()
      childNode = c
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case ChildUpdate(_, a) => {
      emitCreated()
      childNode = a
      nodeData = UnaryNodeData(name, requirements, context, childNode, parentNode)
    }
    case Move(actorRef) => {
      moveTo(actorRef)
    }
    case KillMe => sender() ! PoisonPill
    case Controller(c) => controller = c
    case _: Event =>
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }
}
