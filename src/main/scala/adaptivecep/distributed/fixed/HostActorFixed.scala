package adaptivecep.distributed.fixed

import java.time._
import java.util.concurrent.TimeUnit

import adaptivecep.data.Events._
import akka.actor.{Actor, ActorLogging, ActorRef}
import akka.cluster.Cluster
import akka.cluster.ClusterEvent._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration

class HostActorFixed extends Actor with ActorLogging {

  val cluster = Cluster(context.system)
  val interval = 5
  var neighbors: Set[ActorRef] = Set.empty[ActorRef]
  var node: ActorRef = self
  var delay: Boolean = false
  val clock: Clock = Clock.systemDefaultZone
  var latencies: Map[ActorRef, scala.concurrent.duration.Duration] = Map.empty[ActorRef, scala.concurrent.duration.Duration]

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
  }
  override def postStop(): Unit = cluster.unsubscribe(self)

  def startLatencyMonitoring(): Unit = context.system.scheduler.schedule(
    initialDelay = FiniteDuration(0, TimeUnit.SECONDS),
    interval = FiniteDuration(interval, TimeUnit.SECONDS),
    runnable = () => {
      neighbors.foreach{ _ ! LatencyRequest(clock.instant)}
      //println(latencies)
    })

  startLatencyMonitoring()

  def delay(delay: Boolean): Unit = {
    node ! Delay(delay)
    this.delay = delay
  }

  def receive = {
    case MemberUp(member) =>
      log.info("Member is Up: {}", member.address)
      //context.system.actorSelection(member.address.toString + "/user/Host") ! LatencyRequest(clock.instant())
    case UnreachableMember(member) =>
      log.info("Member detected as unreachable: {}", member)
    case MemberRemoved(member, previousStatus) =>
      log.info("Member is Removed: {} after {}",
        member.address, previousStatus)
    case LatencyRequest(time) =>
      if(sender() != self){
        if(delay) {
          sender() ! LatencyResponse(time.minusMillis(40))
        } else {
          sender() ! LatencyResponse(time)
        }
        //otherHosts += sender()
        //println(otherHosts)
      }
    case LatencyResponse(requestTime) =>
      if(sender() != self) {
        latencies += sender() -> FiniteDuration(Duration.between(requestTime, clock.instant).dividedBy(2).toMillis, TimeUnit.MILLISECONDS)
        //neighbors += sender()
      }
    case Neighbors(n)=> neighbors = n
    case AllHosts => {
      context.system.actorSelection(self.path.address.toString + "/user/Placement") ! Hosts(neighbors)
      println("sending Hosts", sender(), Hosts(neighbors + self))
    }
    case Ping => sender() ! Ping
    case Delay(b) => delay(b)
    case Node(actorRef) =>{
      node = actorRef
      node ! Delay(delay)
    }
    case HostPropsRequest => sender() ! HostPropsResponse(latencies)
    case _ =>
  }
}