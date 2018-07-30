package adaptivecep.graph.nodes

import akka.actor.{ActorRef, Address, Deploy, PoisonPill, Props}
import com.espertech.esper.client._
import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes.traits._
import adaptivecep.graph.nodes.traits.EsperEngine._
import adaptivecep.graph.qos._
import akka.remote.RemoteScope

case class ConjunctionNode(
    //query: ConjunctionQuery,
    requirements: Set[Requirement],
    queryLength1: Int,
    queryLength2: Int,
    publishers: Map[String, ActorRef],
    frequencyMonitorFactory: MonitorFactory,
    latencyMonitorFactory: MonitorFactory,
    createdCallback: Option[() => Any],
    eventCallback: Option[(Event) => Any])
  extends BinaryNode with EsperEngine {

  override val esperServiceProviderUri: String = name

  var childNode1Created: Boolean = false
  var childNode2Created: Boolean = false
  var parentReceived: Boolean = false

  def moveTo(a: ActorRef): Unit = {
    a ! Parent(parentNode)
    a ! Child2(childNode1, childNode2)
    childNode1 ! Parent(a)
    childNode2 ! Parent(a)
    parentNode ! ChildUpdate(self, a)
    childNode1 ! KillMe
  }

  override def receive: Receive = {
    case DependenciesRequest =>
      sender ! DependenciesResponse(Seq(childNode1, childNode2))
    case Created if sender() == childNode1 =>
      childNode1Created = true
      //if (childNode2Created && parentReceived && !created) emitCreated()
    case Created if sender() == childNode2 =>
      childNode2Created = true
      //if (childNode1Created && parentReceived && !created) emitCreated()
    case event: Event if sender() == childNode1 => event match {
      case Event1(e1) => sendEvent("sq1", Array(toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq1", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case event: Event if sender() == childNode2 => event match {
      case Event1(e1) => sendEvent("sq2", Array(toAnyRef(e1)))
      case Event2(e1, e2) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2)))
      case Event3(e1, e2, e3) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3)))
      case Event4(e1, e2, e3, e4) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4)))
      case Event5(e1, e2, e3, e4, e5) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5)))
      case Event6(e1, e2, e3, e4, e5, e6) => sendEvent("sq2", Array(toAnyRef(e1), toAnyRef(e2), toAnyRef(e3), toAnyRef(e4), toAnyRef(e5), toAnyRef(e6)))
    }
    case CentralizedCreated =>
      if(!created){
        created = true
        emitCreated()
      }
    case Parent(p1) => {
      parentNode = p1
      parentReceived = true
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
      //if (childNode1Created && childNode2Created && !created) emitCreated()
    }
    case Child2(c1, c2) => {
      emitCreated()
      childNode1 = c1
      childNode2 = c2
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
    }
    case Move(a) => {
      moveTo(a)
    }
    case ChildUpdate(old, a) => {
      emitCreated()
      if (childNode1.eq(old)) {
        childNode1 = a
      }
      if (childNode2.eq(old)) {
        childNode2 = a
      }
      nodeData = BinaryNodeData(name, requirements, context, childNode1, childNode2, parentNode)
    }
    case Controller(c) => controller = c
    case KillMe => sender() ! PoisonPill
    case _: Event =>
    case unhandledMessage =>
      frequencyMonitor.onMessageReceive(unhandledMessage, nodeData)
      latencyMonitor.onMessageReceive(unhandledMessage, nodeData)
  }

  override def postStop(): Unit = {
    destroyServiceProvider()
  }
  addEventType("sq1", createArrayOfNames(queryLength1), createArrayOfClasses(queryLength1))
  addEventType("sq2", createArrayOfNames(queryLength2), createArrayOfClasses(queryLength2))

/*
  addEventType("sq1", createArrayOfNames(query.sq1), createArrayOfClasses(query.sq1))
  addEventType("sq2", createArrayOfNames(query.sq2), createArrayOfClasses(query.sq2))
*/
  val epStatement: EPStatement = createEpStatement("select * from pattern [every (sq1=sq1 and sq2=sq2)]")

  val updateListener: UpdateListener = (newEventBeans: Array[EventBean], _) => newEventBeans.foreach(eventBean => {
    val values: Array[Any] =
      eventBean.get("sq1").asInstanceOf[Array[Any]] ++
      eventBean.get("sq2").asInstanceOf[Array[Any]]
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