package adaptivecep.distributed.random

import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import adaptivecep.distributed._
import adaptivecep.data.Queries.{Operator => _, _}
import adaptivecep.distributed.operator.{Host, NoHost, NodeHost, Operator}
import adaptivecep.graph.nodes._
import adaptivecep.graph.qos.MonitorFactory
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Address, Deploy, PoisonPill, Props}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._
import akka.remote.RemoteScope

import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.math.Ordering.Implicits.infixOrderingOps

case class PlacementActor (actorSystem: ActorSystem,
                           query: Query,
                           publishers: Map[String, ActorRef],
                           publisherOperators: Map[String, Operator],
                           frequencyMonitorFactory: MonitorFactory,
                           latencyMonitorFactory: MonitorFactory,
                           here: NodeHost)
  extends Actor with ActorLogging{


  case class HostId(id: Int) extends Host

  case class HostProps(latency: Seq[(Host, Duration)], bandwidth: Seq[(Host, Double)])

  sealed trait Optimizing
  case object Maximizing extends Optimizing
  case object Minimizing extends Optimizing

  val cluster = Cluster(context.system)
  var previousPlacement: Map[Operator, Host] = Map.empty[Operator, Host]

  var propsOperators: Map[Props, Operator] = Map.empty[Props ,Operator]
  var propsActors: Map[Props, ActorRef] = Map.empty[Props, ActorRef]
  var parents: Map[Operator, Option[Operator]] = Map.empty[Operator, Option[Operator]] withDefaultValue None

  var hostProps: Map[Host, HostProps] = Map.empty[Host, HostProps]
  var consumers: Seq[Operator] = Seq.empty[Operator]
  var hostMap: Map[ActorRef, Host] = Map((here.actorRef) -> here)

  val interval = 5

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

  override def postStop(): Unit ={
    propsActors.keys.foreach(key => propsActors(key) ! PoisonPill)
    cluster.unsubscribe(self)
  }

  def run(): Unit = {
    placeOptimizingLatency()
  }

  def placeAll(map: Map[Operator, Host])={
    map.foreach(pair => place(pair._1, pair._2))
    map.keys.foreach(operator => {
      if (operator.props != null) {
        val actorRef = propsActors(operator.props)
        val children = operator.dependencies
        children.length match {
          case 0 =>
          case 1 =>
            if (children.head.props != null) {
              actorRef ! Child1(propsActors(children.head.props))
              actorRef ! CentralizedCreated
            }
          case 2 =>
            if (children.head.props != null && children(1).props != null) {
              actorRef ! Child2(propsActors(children.head.props), propsActors(children(1).props))
              actorRef ! CentralizedCreated
            }
        }
        val parent = parents(operator)
        if (parent.isDefined) {
          actorRef ! Parent(propsActors(parent.get.props))
          //println("setting Parent of", actorRef, propsActors(parent.get.props))
        }
      }
    })
    previousPlacement = map
  }

  def place(operator: Operator, host: Host): Unit={
    if(host != NoHost && operator.props != null){
      val moved = previousPlacement.contains(operator) && previousPlacement(operator) != host
      if(moved) {
        propsActors(operator.props) ! PoisonPill
        println("killing old actor", propsActors(operator.props))
      }
      if (moved || previousPlacement.isEmpty){
        val ref = actorSystem.actorOf(operator.props.withDeploy(Deploy(scope = RemoteScope(host.asInstanceOf[NodeHost].actorRef.path.address))))
        ref ! Controller(self)
        propsActors += operator.props -> ref
        println("placing Actor", ref)
      }
    }
  }

  override def receive: Receive = {
    case InitializeQuery => {
      context.system.scheduler.schedule(
        initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
        interval = FiniteDuration(interval, TimeUnit.SECONDS),
        runnable = () => {
          hostMap.foreach{
            host => host._2.asInstanceOf[NodeHost].actorRef ! HostPropsRequest
            //println("PLACEMENT ACTOR: sending HostPropsRequest to", host)
          }
        })
 //     initialize(query, publishers, frequencyMonitorFactory, latencyMonitorFactory, Some(eventCallback), consumer = true)
      context.system.actorSelection(self.path.address + "/user/Host-14") ! AllHosts
    }
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case MemberExited(member) =>
      log.info("Member exiting: {}", member)
      checkAndRestoreActor(member.address)
    case RequirementsNotMet =>
      propsActors.values.foreach(actorRef => if(sender().equals(actorRef)){
        println("Recalculating Placement", sender())
        run()
      })
    case Start =>
      println("PLACEMENT ACTOR: starting")
      run()
    case Hosts(hostss) =>
      //here = NodeHost(sender())
      var latencyStub: Seq[(Host, Duration)] = Seq.empty[(Host, Duration)]
      hostss.foreach(host => {
        if(!hostMap.contains(host)) {
          val nodeHost = NodeHost(host)
          hostMap += host -> nodeHost
          latencyStub = latencyStub :+ (nodeHost, Duration.Inf)
        }
      })
      if(!hostProps.contains(NoHost)){
        hostProps += NoHost -> HostProps(latencyStub, Seq.empty[(Host, Double)])
      }
      hostProps(NoHost).latency ++ latencyStub
    case HostPropsResponse(latencies) => {
      //println("PLACEMENT ACTOR: got HostPropsResponse from", sender())
      //println(hosts)
      //println(latencies)
      var test = Seq.empty[(Host, Duration)]
      latencies.foreach(tuple => if (hostMap.contains(tuple._1)) {
        if(hostMap.contains(tuple._1)) {
 //         test = test :+ (hostMap(tuple._1), tuple._2)
        }
      })
      if (hostMap.contains(sender())) {
        hostProps += hostMap(sender()) -> HostProps(test, Seq.empty[(Host, Double)])
      }
    }
  }

  def moveToAddress(props: Props, address: Address): Unit = {
    val oldActor = propsActors(props)
    val migratedActor = actorSystem.actorOf(props.withDeploy(Deploy(scope = RemoteScope(address))))
    propsActors += props -> migratedActor
   // oldActor ! Move(migratedActor)
  }


  def checkAndRestoreActor(address: Address): Unit ={
    propsActors.foreach(
      tuple => if(tuple._2.path.address.eq(address)){
        val oldProps = tuple._1
        val oldActor = tuple._2
        val newAddress = Address("akka.tcp", "ClusterSystem", "127.0.0.1", 2551)
        val restoredActor = actorSystem.actorOf(oldProps.withDeploy(Deploy(scope = RemoteScope(newAddress))))

        if (parents(propsOperators(oldProps)).isDefined){
          val parent = parents(propsOperators(oldProps)).get
          restoredActor ! Parent(propsActors(parent.props))
          propsActors(parent.props) ! ChildUpdate(oldActor, restoredActor)
        }
        if(propsOperators(oldProps).dependencies.nonEmpty) {
          propsOperators(oldProps).dependencies length match {
            case 1 => {
              val child = propsActors(propsOperators(oldProps).dependencies.head.props)
              restoredActor ! Child1(child)
              child ! Parent(restoredActor)
            }
            case 2 => {
              val child1 = propsActors(propsOperators(oldProps).dependencies.head.props)
              val child2 = propsActors(propsOperators(oldProps).dependencies(1).props)
              restoredActor! Child2(child1, child2)
              child1 ! Parent(restoredActor)
              child2 ! Parent(restoredActor)
            }
          }
        }
    })
  }

  private def latencySelector(props: HostProps, host: Host): Duration = {
    if(host.equals(NoHost)){
      return Duration.apply(50, TimeUnit.DAYS)
    }
    val latency = (props.latency collectFirst { case (`host`, latency) => latency })
    if(latency.isDefined){
      latency.get
    }
    else Duration(50, TimeUnit.DAYS)

  }

  private def bandwidthSelector(props: HostProps, host: Host): Double =
    (props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }).get

  private def latencyBandwidthSelector(props: HostProps, host: Host): (Duration, Double) =
    ((props.latency collectFirst { case (`host`, latency) => latency }).get,
      (props.bandwidth collectFirst { case (`host`, bandwidth) => bandwidth }).get)

  private def avg(durations: Seq[Duration]): Duration =
    if (durations.isEmpty)
      Duration.Zero
    else
      durations.foldLeft[Duration](Duration.Zero) { _ + _ } / durations.size

  private def avg(numerics: Seq[Double]): Double =
    if (numerics.isEmpty)
      0.0
    else
      numerics.sum / numerics.size
  /*
  def measureLatency: Duration =
    measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avg } { _.host }

  def measureBandwidth: Double =
    measure(bandwidthSelector, Maximizing, Double.MaxValue) { math.min } { avg } { _.host }
    */

  private def measure[T: Ordering](
                                    selector: (HostProps, Host) => T,
                                    optimizing: Optimizing,
                                    zero: T)(
                                    merge: (T, T) => T)(
                                    avg: Seq[T] => T)(
                                    host: Operator => Host): T = {
    def measure(operator: Operator): T =
      if (operator.dependencies.isEmpty)
        zero
      else
        minmax(optimizing, operator.dependencies map { dependentOperator =>
          merge(measure(dependentOperator), selector(hostProps(host(operator)), host(dependentOperator)))
        })

    avg(consumers map measure)
  }

  def placeOptimizingLatency(): Unit = {
    val measureLatency = measure(latencySelector, Minimizing, Duration.Zero) { _ + _ } { avg } _

    val placementsA = placeOptimizingHeuristicA(latencySelector, Minimizing)
    val durationA = measureLatency { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencySelector, Minimizing) { _ + _ }
    val durationB = measureLatency { placementsB(_) }

    placeAll((if (durationA < durationB) placementsA else placementsB).toMap)

      /*
      case (operator, host) =>
      place(operator, host)*/

  }

  def placeOptimizingBandwidth(): Unit = {
    val measureBandwidth = measure(bandwidthSelector, Maximizing, Double.MaxValue) { math.min } { avg } _

    val placementsA = placeOptimizingHeuristicA(bandwidthSelector, Maximizing)
    val bandwidthA = measureBandwidth { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(bandwidthSelector, Maximizing) { math.min }
    val bandwidthB = measureBandwidth { placementsB(_) }

    (if (bandwidthA > bandwidthB) placementsA else placementsB) foreach { case (operator, host) =>
      place (operator, host)
    }
  }

  def placeOptimizingLatencyAndBandwidth(): Unit = {
    def average(durationNumerics: Seq[(Duration, Double)]): (Duration, Double) =
      durationNumerics.unzip match { case (latencies, bandwidths) => (avg(latencies), avg(bandwidths)) }

    def merge(durationNumeric0: (Duration, Double), durationNumeric1: (Duration, Double)): (Duration, Double) =
      (durationNumeric0, durationNumeric1) match { case ((duration0, numeric0), (duration1, numeric1)) =>
        (duration0 + duration1, math.min(numeric0, numeric1))
      }

    implicit val ordering = new Ordering[(Duration, Double)] {
      def abs(x: Duration) = if (x < Duration.Zero) -x else x
      def compare(x: (Duration, Double), y: (Duration, Double)) = ((-x._1, x._2), (-y._1, y._2)) match {
        case ((d0, n0), (d1, n1)) if d0 == d1 && n0 == n1 => 0
        case ((d0, n0), (d1, n1)) if d0 < d1 && n0 < n1 => -1
        case ((d0, n0), (d1, n1)) if d0 > d1 && n0 > n1 => 1
        case ((d0, n0), (d1, n1)) =>
          math.signum((d0 - d1) / abs(d0 + d1) + (n0 - n1) / math.abs(n0 + n1)).toInt
      }
    }

    val measureBandwidth = measure(latencyBandwidthSelector, Maximizing, (Duration.Zero, Double.MaxValue)) { merge } { average } _

    val placementsA = placeOptimizingHeuristicA(latencyBandwidthSelector, Maximizing)
    val bandwidthA = measureBandwidth { placementsA(_) }

    val placementsB = placeOptimizingHeuristicB(latencyBandwidthSelector, Maximizing) { merge }
    val bandwidthB = measureBandwidth { placementsB(_) }

    (if (bandwidthA > bandwidthB) placementsA else placementsB) foreach { case (operator, host) =>
      place (operator, host)
    }
  }

  private def placeOptimizingHeuristicA[T: Ordering](
                                                      selector: (HostProps, Host) => T,
                                                      optimizing: Optimizing): collection.Map[Operator, Host] = {
    val placements = mutable.Map.empty[Operator, Host]

    def placeProducersConsumers(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeProducersConsumers(_, consumer = false) }
      if (consumer || operator.dependencies.isEmpty)
        placements += operator -> operator.host
    }

    def placeIntermediates(operator: Operator, consumer: Boolean): Unit = {
      operator.dependencies foreach { placeIntermediates(_, consumer = false) }

      val host =
        if (!consumer && operator.dependencies.nonEmpty) {
          val valuesForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _== host }) =>
              val propValues =
                operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }

              minmax(optimizing, propValues) -> host
            }

          if (valuesForHosts.isEmpty)
            throw new UnsupportedOperationException("not enough hosts")

          val (_, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }
          host
        }
        else
          operator.host

      placements += operator -> host
    }

    consumers foreach { placeProducersConsumers(_, consumer = true) }
    consumers foreach { placeIntermediates(_, consumer = true) }
    //println("PLACEMENT ACTOR: HeuristicA - ", placements)
    placements
  }

  private def placeOptimizingHeuristicB[T: Ordering](
                                                      selector: (HostProps, Host) => T,
                                                      optimizing: Optimizing)(
                                                      merge: (T, T) => T): collection.Map[Operator, Host] = {
    val previousPlacements = mutable.Map.empty[Operator, mutable.Set[Host]]
    val placements = mutable.Map.empty[Operator, Host]

    def allOperators(operator: Operator, parent: Option[Operator]): Seq[(Operator, Option[Operator])] =
      (operator -> parent) +: (operator.dependencies flatMap { allOperators(_, Some(operator)) })

    val operators = consumers flatMap { allOperators(_, None) }
    operators foreach { case (operator, _) =>
      placements += operator -> operator.host
      previousPlacements += operator -> mutable.Set(operator.host)
    }

    @tailrec def placeOperators(): Unit = {
      val changed = operators map {
        case (operator, Some(parent)) if operator.dependencies.nonEmpty =>
          val valuesForHosts =
            hostProps.toSeq collect { case (host, props) if !(placements.values exists { _ == host }) && !(previousPlacements(operator) contains host) =>
              merge(
                minmax(optimizing, operator.dependencies map { dependentOperator =>
                  selector(props, placements(dependentOperator))
                }),
                selector(hostProps(placements(parent)), host)) -> host
            }

          val currentValue =
            merge(
              minmax(optimizing, operator.dependencies map { dependency =>
                selector(hostProps(placements(operator)), placements(dependency))
              }),
              selector(hostProps(placements(parent)), placements(operator)))

          val noPotentialPlacements =
            if (valuesForHosts.isEmpty) {
              if ((hostProps.keySet -- placements.values --previousPlacements(operator)).isEmpty)
                true
              else
                throw new UnsupportedOperationException("not enough hosts")
            }
            else
              false

          if (!noPotentialPlacements) {
            val (value, host) = minmaxBy(optimizing, valuesForHosts) { case (value, _) => value }

            val changePlacement = value < currentValue
            if (changePlacement) {
              placements += operator -> host
              previousPlacements(operator) += host
            }

            changePlacement
          }
          else
            false

        case _ =>
          false
      }

      if (changed contains true)
        placeOperators()
    }
    placeOperators()

    //println("PLACEMENT ACTOR Heuristic B", placements)
    placements
  }

  private def minmax[T: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T]): T = optimizing match {
    case Maximizing => traversable.min
    case Minimizing => traversable.max
  }

  private def minmaxBy[T, U: Ordering](optimizing: Optimizing, traversable: TraversableOnce[T])(f: T => U): T = optimizing match {
    case Maximizing => traversable maxBy f
    case Minimizing => traversable minBy f
  }
/*
  def initialize(query: Query,
                 publishers: Map[String, ActorRef],
                 frequencyMonitorFactory: MonitorFactory,
                 latencyMonitorFactory: MonitorFactory,
                 callback: Option[Event => Any],
                 consumer: Boolean ): Props = {
    query match {
      case streamQuery: StreamQuery =>
        initializeStreamQuery(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  streamQuery, consumer)
      case sequenceQuery: SequenceQuery =>
        initializeSequenceNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  sequenceQuery, consumer)
      case filterQuery: FilterQuery =>
        initializeFilterNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  filterQuery, consumer)
      case dropElemQuery: DropElemQuery =>
        initializeDropElemNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  dropElemQuery, consumer)
      case selfJoinQuery: SelfJoinQuery =>
        initializeSelfJoinNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  selfJoinQuery, consumer)
      case joinQuery: JoinQuery =>
        initializeJoinNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  joinQuery, consumer)
      case conjunctionQuery: ConjunctionQuery =>
        initializeConjunctionNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback,  conjunctionQuery, consumer)
      case disjunctionQuery: DisjunctionQuery =>
        initializeDisjunctionNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, callback, disjunctionQuery, consumer)
    }
  }*/
/*
  private def initializeStreamQuery(publishers: Map[String, ActorRef],
                                    frequencyMonitorFactory: MonitorFactory,
                                    latencyMonitorFactory: MonitorFactory,
                                    callback: Option[Event => Any],
                                    streamQuery: StreamQuery,
                                    consumer: Boolean) = {
    val props = Props(
      StreamNode(
        streamQuery.requirements,
        streamQuery.publisherName, publishers,
        frequencyMonitorFactory,
        latencyMonitorFactory,
        None,
        callback))
    propsOperators += props -> ActiveOperator(NoHost, props, Seq(publisherOperators(streamQuery.publisherName)))
    props
  }

  private def initializeDisjunctionNode(publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        disjunctionQuery: DisjunctionQuery,
                                        consumer: Boolean) = {
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
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, disjunctionQuery.sq1, disjunctionQuery.sq2, props, consumer)
    props
  }


  private def initializeConjunctionNode(publishers: Map[String, ActorRef],
                                        frequencyMonitorFactory: MonitorFactory,
                                        latencyMonitorFactory: MonitorFactory,
                                        callback: Option[Event => Any],
                                        conjunctionQuery: ConjunctionQuery,
                                        consumer: Boolean) = {
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
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, conjunctionQuery.sq1, conjunctionQuery.sq2, props, consumer)
    props
  }

  private def initializeJoinNode(publishers: Map[String, ActorRef],
                                 frequencyMonitorFactory: MonitorFactory,
                                 latencyMonitorFactory: MonitorFactory,
                                 callback: Option[Event => Any],
                                 joinQuery: JoinQuery,
                                 consumer: Boolean) = {
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
    connectBinaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, joinQuery.sq1, joinQuery.sq2, props, consumer)
    props
  }

  private def initializeSelfJoinNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     selfJoinQuery: SelfJoinQuery,
                                     consumer: Boolean) = {
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
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, selfJoinQuery.sq, props, consumer)
    props
  }

  private def initializeDropElemNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     dropElemQuery: DropElemQuery,
                                     consumer: Boolean) = {
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
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, dropElemQuery.sq, props, consumer)
    props
  }

  private def initializeFilterNode(publishers: Map[String, ActorRef],
                                   frequencyMonitorFactory: MonitorFactory,
                                   latencyMonitorFactory: MonitorFactory,
                                   callback: Option[Event => Any],
                                   filterQuery: FilterQuery,
                                   consumer: Boolean) = {
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
    connectUnaryNode(publishers, frequencyMonitorFactory, latencyMonitorFactory, filterQuery.sq, props, consumer)
    props
  }

  private def initializeSequenceNode(publishers: Map[String, ActorRef],
                                     frequencyMonitorFactory: MonitorFactory,
                                     latencyMonitorFactory: MonitorFactory,
                                     callback: Option[Event => Any],
                                     sequenceQuery: SequenceQuery,
                                     consumer: Boolean) = {
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
    propsOperators += props -> ActiveOperator(NoHost, props, Seq(publisherOperators(sequenceQuery.s1.publisherName), publisherOperators(sequenceQuery.s2.publisherName)))
    props
  }
*//*
  private def connectUnaryNode(publishers: Map[String, ActorRef],
                               frequencyMonitorFactory: MonitorFactory,
                               latencyMonitorFactory: MonitorFactory,
                               query: Query, props: Props,
                               consumer: Boolean) : Unit = {
    val child = initialize(query, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory, None, consumer = false)
    val childOperator = propsOperators(child)
    var operator = ActiveOperator(here, props, Seq(childOperator))
    if(consumer){
      consumers = consumers :+ operator
    }
    operator = ActiveOperator(NoHost, props, Seq(childOperator))
    propsOperators += props -> operator
    parents += childOperator -> Some(propsOperators(props))

  }

  private def connectBinaryNode(publishers: Map[String, ActorRef],
                                frequencyMonitorFactory: MonitorFactory,
                                latencyMonitorFactory: MonitorFactory,
                                query1: Query,
                                query2: Query,
                                props: Props,
                                consumer: Boolean) : Unit = {
    val child1 = initialize(query1, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory, None, consumer = false)
    val child2 = initialize(query2, publishers,
      frequencyMonitorFactory,
      latencyMonitorFactory, None, consumer = false)
    val child1Operator = propsOperators(child1)
    val child2Operator = propsOperators(child2)
    var operator = ActiveOperator(here, props, Seq(child1Operator, child2Operator))
    if(consumer){
      consumers = consumers :+ operator
    }
    operator = ActiveOperator(NoHost, props, Seq(child1Operator, child2Operator))
    propsOperators += props -> operator
    parents += child1Operator -> Some(propsOperators(props))
    parents += child2Operator -> Some(propsOperators(props))
  }
*/
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
