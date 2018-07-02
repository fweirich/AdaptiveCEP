package adaptivecep.distributed

import adaptivecep.data.Events._
import adaptivecep.data.Queries._
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos.MonitorFactory
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Deploy, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.RemoteScope

import scala.collection.mutable.ListBuffer

case class PlacementActor (actorSystem: ActorSystem,
                           query: Query,
                           publishers: Map[String, ActorRef],
                           frequencyMonitorFactory: MonitorFactory,
                           latencyMonitorFactory: MonitorFactory)
  extends Actor with ActorLogging{

  case class ConnectionData(
    var props: Props,
    var address: Address,
    var children: Option[Child] = None,
    var parent: Option[ActorRef] = None
  )

  val cluster = Cluster(context.system)
  var connections: Map[ActorRef, ConnectionData] = Map.empty[ActorRef, ConnectionData]
  var availableAddresses: ListBuffer[Address] = ListBuffer.empty[Address]

  /*
  var actorToPropsMap: Map[ActorRef, Props] = Map.empty[ActorRef, Props]
  var actorToAddressMap: Map[ActorRef, Address] = Map.empty[ActorRef, Address]
  var children: Map[ActorRef, Child] = Map.empty[ActorRef, Child]
  var parents: Map[ActorRef, ActorRef] = Map.empty[ActorRef, ActorRef]
  */


  //val createdCallback: Option[() => Any] = () => println("STATUS:\t\tGraph has been created.")
  val eventCallback: (Event) => Any = {
      // Callback for `query1`:
      case Event3(Left(i1), Left(i2), Left(f)) => println(s"COMPLEX EVENT:\tEvent3($i1,$i2,$f)")
      case Event3(Right(s), _, _)              => println(s"COMPLEX EVENT:\tEvent1($s)")
      // Callback for `query2`:
      // case (i1, i2, f, s)             => println(s"COMPLEX EVENT:\tEvent4($i1, $i2, $f,$s)")
      // This is necessary to avoid warnings about non-exhaustive `match`:
      case _                             => println("what the hell")
  }

  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)


  override def receive: Receive = {
    case InitializeQuery => {
      val address: Address = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2551)
      initialize(query, publishers, frequencyMonitorFactory, latencyMonitorFactory, Some(eventCallback), address)
      println("Query Initialized")
    }
    case "Move" =>
      /*connections.keys.foreach(actor => {
        val address = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2553)
        moveToAddress(actor, address)
        //Thread.sleep(3000)
      })*/
      val address = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2553)
      moveToAddress(connections.keys.head, address)
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      availableAddresses += member.address
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case MemberExited(member) =>
      log.info("Member exiting: {}", member)
      availableAddresses -= member.address
      checkAndRestoreActor(member.address)
  }

  def moveToAddress(actorRef: ActorRef, address: Address): Unit = {
    val oldData = connections(actorRef)
    val migratedActor = actorSystem.actorOf(oldData.props.withDeploy(Deploy(scope = RemoteScope(address))))
    connections += migratedActor -> ConnectionData(oldData.props, address, oldData.children, oldData.parent)
    connections -= actorRef
    actorRef ! Move(migratedActor)
  }


  def checkAndRestoreActor(address: Address): Unit ={
    connections.foreach(
      tuple => if(tuple._2.address.eq(address)){
        val oldActor = tuple._1
        val oldData = tuple._2
        val newAddress = availableAddresses(1)
        val restoredActor = actorSystem.actorOf(oldData.props.withDeploy(Deploy(scope = RemoteScope(newAddress))))

        if (oldData.parent.isDefined){
          val parent = oldData.parent.get
          restoredActor ! Parent(parent)
          parent ! ChildUpdate(oldActor, restoredActor)
        }
        if(oldData.children.isDefined) {
          val child: Child = oldData.children.get
          restoredActor ! child
          child match {
            case Child1(c) => {
              c ! Parent(restoredActor)
            }
            case Child2(c1, c2) => {
              c1 ! Parent(restoredActor)
              c2 ! Parent(restoredActor)
            }
          }
        }
    })
  }



  def initialize(query: Query,
                 publishers: Map[String, ActorRef],
                 frequencyMonitorFactory: MonitorFactory,
                 latencyMonitorFactory: MonitorFactory,
                 callback: Option[Event => Any],
                 address: Address): ActorRef = query match {
    case streamQuery: StreamQuery =>
      initializeStreamQuery(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, streamQuery)
    case sequenceQuery: SequenceQuery =>
      initializeSequenceNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, sequenceQuery)
    case filterQuery: FilterQuery =>
      initializeFilterNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, filterQuery)
    case dropElemQuery: DropElemQuery =>
      initializeDropElemNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, dropElemQuery)
    case selfJoinQuery: SelfJoinQuery =>
      initializeSelfJoinNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, selfJoinQuery)
    case joinQuery: JoinQuery =>
      initializeJoinNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, joinQuery)
    case conjunctionQuery: ConjunctionQuery =>
      initializeConjunctionNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, conjunctionQuery)
    case disjunctionQuery: DisjunctionQuery =>
      initializeDisjunctionNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, address, disjunctionQuery)
  }

  private def initializeStreamQuery(publishers: Map[String, ActorRef],
                                    frequencyMonitorFactory: MonitorFactory,
                                    latencyMonitorFactory: MonitorFactory,
                                    callback: Option[Event => Any],
                                    address: Address,
                                    streamQuery: StreamQuery) = {
    val props = Props(
      StreamNode(
        streamQuery.requirements,
        streamQuery.publisherName, publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    val leaf = actorSystem.actorOf(props.withDeploy(Deploy(scope = RemoteScope(address))))
    connections += leaf -> ConnectionData(props, address)
    leaf
  }

  private def initializeDisjunctionNode(publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        address: Address,
                                        disjunctionQuery: DisjunctionQuery) = {
    val length = getQueryLength(disjunctionQuery)
    val props = Props(
      DisjunctionNode(
        disjunctionQuery.requirements,
        length,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, address, disjunctionQuery.sq1, disjunctionQuery.sq2, props)
  }

  private def initializeConjunctionNode(publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        address: Address,
                                        conjunctionQuery: ConjunctionQuery) = {
    val length1 = getQueryLength(conjunctionQuery.sq1)
    val length2 = getQueryLength(conjunctionQuery.sq2)
    val props = Props(
      ConjunctionNode(
        conjunctionQuery.requirements,
        length1,
        length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, address, conjunctionQuery.sq1, conjunctionQuery.sq2, props)
  }

  private def initializeJoinNode(publishers: Map[String, ActorRef],
                                 frequencyMonitorFactory: MonitorFactory,
                                 latencyMonitorFactory: MonitorFactory,
                                 callback: Option[Event => Any],
                                 address: Address,
                                 joinQuery: JoinQuery) = {
    val wt1 = getWindowType(joinQuery.w1)
    val ws1 = getWindowSize(joinQuery.w1)
    val wt2 = getWindowType(joinQuery.w2)
    val ws2 = getWindowSize(joinQuery.w2)
    val length1 = getQueryLength(joinQuery.sq1)
    val length2 = getQueryLength(joinQuery.sq2)
    val props = Props(
      JoinNode(
        joinQuery.requirements,
        wt1, ws1, wt2, ws2, length1, length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, address, joinQuery.sq1, joinQuery.sq2, props)
  }

  private def initializeSelfJoinNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     address: Address,
                                     selfJoinQuery: SelfJoinQuery) = {
    val wt1 = getWindowType(selfJoinQuery.w1)
    val ws1 = getWindowSize(selfJoinQuery.w1)
    val wt2 = getWindowType(selfJoinQuery.w2)
    val ws2 = getWindowSize(selfJoinQuery.w2)
    val length = getQueryLength(selfJoinQuery)
    val props = Props(
      SelfJoinNode(
        selfJoinQuery.requirements,
        wt1, ws1, wt2, ws2, length,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, address, selfJoinQuery.sq, props)
  }

  private def initializeDropElemNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     address: Address,
                                     dropElemQuery: DropElemQuery) = {
    val drop = elemToBeDropped(dropElemQuery)
    val props = Props(
      DropElemNode(
        dropElemQuery.requirements,
        drop,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, address, dropElemQuery.sq, props)
  }

  private def initializeFilterNode(publishers: Map[String, ActorRef],
                                   frequencyMonitorFactory: MonitorFactory,
                                   latencyMonitorFactory: MonitorFactory,
                                   callback: Option[Event => Any],
                                   address: Address,
                                   filterQuery: FilterQuery) = {
    val cond = filterQuery.cond
    val props = Props(
      FilterNode(
        filterQuery.requirements,
        cond.asInstanceOf[Event => Boolean],
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, address, filterQuery.sq, props)
  }

  private def initializeSequenceNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     address: Address,
                                     sequenceQuery: SequenceQuery) = {
    val length1 = getQueryLength(sequenceQuery.s1)
    val length2 = getQueryLength(sequenceQuery.s2)
    val props = Props(
      SequenceNode(
        sequenceQuery.requirements,
        sequenceQuery.s1.publisherName,
        sequenceQuery.s2.publisherName,
        length1,
        length2,
        publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    val leaf = actorSystem.actorOf(props)
    connections += leaf -> ConnectionData(props, address)
    leaf
  }

  private def connectUnaryNode(publishers: Map[String, ActorRef],
                               frequencyMonitorFactory: MonitorFactory,
                               latencyMonitorFactory: MonitorFactory,
                               address: Address, query: Query, props: Props) = {
    val parent: ActorRef = actorSystem.actorOf(props)
    val child = initialize(query, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory, None, address)
    parent ! Child1(child)
    child ! Parent(parent)
    connections += parent -> ConnectionData(props, address, Some(Child1(child)))
    connections(child).parent = Some(parent)
    parent
  }

  private def connectBinaryNode(publishers: Map[String, ActorRef],
                                frequencyMonitorFactory: MonitorFactory,
                                latencyMonitorFactory: MonitorFactory,
                                address: Address,
                                query1: Query,
                                query2: Query,
                                props: Props) = {
    val parent: ActorRef = actorSystem.actorOf(props)
    val child1 = initialize(query1, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory, None, address)
    val child2 = initialize(query2, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory, None, address)
    parent ! Child2(child1, child2)
    child1 ! Parent(parent)
    child2 ! Parent(parent)
    connections += parent -> ConnectionData(props, address, Some(Child2(child1, child2)))
    connections(child1).parent = Some(parent)
    connections(child2).parent = Some(parent)
    parent
  }

  def getQueryLength(query: Query): Int = query match {
    case _: Query1[_] => 1
    case _: Query2[_, _] => 2
    case _: Query3[_, _, _] => 3
    case _: Query4[_, _, _, _] => 4
    case _: Query5[_, _, _, _, _] => 5
    case _: Query6[_, _, _, _, _, _] => 6
  }

  def getQueryLength(noReqStream: NStream):Int = noReqStream match {
    case _: NStream1[_] => 1
    case _: NStream2[_, _] => 2
    case _: NStream3[_, _, _] => 3
    case _: NStream4[_, _, _, _] => 4
    case _: NStream5[_, _, _, _, _] => 5
  }
  def elemToBeDropped(query: Query): Int = query match {
    case DropElem1Of2(_, _) => 1
    case DropElem1Of3(_, _) => 1
    case DropElem1Of4(_, _) => 1
    case DropElem1Of5(_, _) => 1
    case DropElem1Of6(_, _) => 1
    case DropElem2Of2(_, _) => 2
    case DropElem2Of3(_, _) => 2
    case DropElem2Of4(_, _) => 2
    case DropElem2Of5(_, _) => 2
    case DropElem2Of6(_, _) => 2
    case DropElem3Of3(_, _) => 3
    case DropElem3Of4(_, _) => 3
    case DropElem3Of5(_, _) => 3
    case DropElem3Of6(_, _) => 3
    case DropElem4Of4(_, _) => 4
    case DropElem4Of5(_, _) => 4
    case DropElem4Of6(_, _) => 4
    case DropElem5Of5(_, _) => 5
    case DropElem5Of6(_, _) => 5
    case DropElem6Of6(_, _) => 6
    case _ => 1
  }

  def getWindowType(window: Window): String = window match{
    case _ : SlidingInstances => "SI"
    case _ : TumblingInstances => "TI"
    case _ : SlidingTime => "ST"
    case _ : TumblingTime => "TT"
  }

  def getWindowSize(window: Window): Int = window match{
    case SlidingInstances(i) => i
    case TumblingInstances(i) => i
    case SlidingTime(i) => i
    case TumblingTime(i) => i
  }

}
