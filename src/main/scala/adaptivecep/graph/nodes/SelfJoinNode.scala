package adaptivecep.graph.nodes

import akka.actor.{ActorRef, Address, Deploy, PoisonPill, Props}
import com.espertech.esper.client._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.qos._
import JoinNode._
import akka.remote.RemoteScope

case class SelfJoinNode(
    //query: SelfJoinQuery,
    requirements: Set[Requirement],
    windowType1: String,
    windowSize1: Int,
    windowType2: String,
    windowSize2: Int,
    queryLength: Int,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends UnaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  /*
  override val publishers = null
  override val createdCallback: Option[() => Any] = null
  override val eventCallback: Option[Event => Any] = null
*/
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
    case event: Event if sender() == childNode => event match {
      case Event1(e1) => sendEvent("sq", Array(toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case Parent(p1) => {
      parentNode = p1
      println("parent received selfjoin", self.path)
    }
    case Child1(c) => {
      childNode = c
      nodeData = UnaryNodeData(name, requirements, context, childNode)
      println("child received", self.path)
    }
    case Move(a) => {
      moveTo(a)
    }
    case ChildUpdate(_, a) => {
      childNode = a
      nodeData = UnaryNodeData(name, requirements, context, childNode)
    }
    case KillMe => sender() ! PoisonPill
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }

  addEventType("sq", createArrayOfNames(queryLength), createArrayOfClasses(queryLength))

  //addEventType("sq", createArrayOfNames(query.sq), createArrayOfClasses(query.sq))

  val epStatement: EPStatement = createEpStatement(
    s"select * from " +
    s"sq.${createWindowEplString(createWindow(windowType1, windowSize1))} as lhs, " +
    s"sq.${createWindowEplString(createWindow(windowType1, windowSize1))} as rhs")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("lhs").asInstanceOf[Array[Any]] ++
      eventBean.get("rhs").asInstanceOf[Array[Any]]
    val event: Event = values.length match {
      case 2 => Event2(values(0), values(1))
      case 3 => Event3(values(0), values(1), values(2))
      case 4 => Event4(values(0), values(1), values(2), values(3))
      case 5 => Event5(values(0), values(1), values(2), values(3), values(4))
      case 6 => Event6(values(0), values(1), values(2), values(3), values(4), values(5))
    }
    emitEvent(event)
  })

  epStatement.addListener(updateListener)

}
