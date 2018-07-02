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

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode))
    case Created if sender() == childNode =>
      emitCreated()
    case event: Event if sender() == childNode => {
      if (cond(event)) emitEvent(event)
      println("gotEvent", cond(event))
    }
    case Parent(p1) => {
      parentNode = p1
      println("parent received filter", self.path)
    }
    case Child1(c) => {
      childNode = c
      nodeData = UnaryNodeData(name, requirements, context, childNode)
      println("child received", self.path)
    }
    case ChildUpdate(_, a) => {
      childNode = a
      nodeData = UnaryNodeData(name, requirements, context, childNode)
    }
    case Move(actorRef) => {
      moveTo(actorRef)
    }
    case KillMe => sender() ! PoisonPill
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

}
