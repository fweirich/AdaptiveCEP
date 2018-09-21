package adaptivecep.distributed.greedy

import adaptivecep.data.Events._
import adaptivecep.data.Queries.Query
import adaptivecep.distributed._
import adaptivecep.distributed.operator.{Host, NoHost, NodeHost, Operator}
import adaptivecep.graph.qos.MonitorFactory
import akka.actor.{ActorRef, ActorSystem, Deploy}
import akka.remote.RemoteScope

case class PlacementActorGreedy (actorSystem: ActorSystem,
                           query: Query,
                           publishers: Map[String, ActorRef],
                           publisherOperators: Map[String, Operator],
                           frequencyMonitorFactory: MonitorFactory,
                           latencyMonitorFactory: MonitorFactory,
                           bandwidthMonitorFactory: MonitorFactory,
                           here: NodeHost,
                           hosts: Set[ActorRef],
                           optimizeFor: String)
  extends PlacementActorBase {

  def placeAll(map: Map[Operator, Host]): Unit ={
    map.foreach(pair => place(pair._1, pair._2))
    map.keys.foreach(operator => {
      if (operator.props != null) {
        val actorRef = propsActors(operator.props)
        val children = operator.dependencies
        children.length match {
          case 0 =>
          case 1 =>
            if (children.head.props != null) {
              map(operator).asInstanceOf[NodeHost].actorRef ! ChildHost1(map(propsOperators(children.head.props)).asInstanceOf[NodeHost].actorRef)
              //map(operator).asInstanceOf[NodeHost].actorRef ! ChildResponse(propsActors(children.head.props))
              actorRef ! Child1(propsActors(children.head.props))
              actorRef ! CentralizedCreated

            }
          case 2 =>
            if (children.head.props != null && children(1).props != null) {
              map(operator).asInstanceOf[NodeHost].actorRef ! ChildHost2(map(propsOperators(children.head.props)).asInstanceOf[NodeHost].actorRef, map(propsOperators(children(1).props)).asInstanceOf[NodeHost].actorRef)
              //map(operator).asInstanceOf[NodeHost].actorRef ! ChildResponse(propsActors(children.head.props))
              //map(operator).asInstanceOf[NodeHost].actorRef ! ChildResponse(propsActors(children(1).props))
              actorRef ! Child2(propsActors(children.head.props), propsActors(children(1).props))
              actorRef ! CentralizedCreated

            }
        }
        val parent = parents(operator)
        if (parent.isDefined) {
          map(operator).asInstanceOf[NodeHost].actorRef ! ParentHost(map(propsOperators(parent.get.props)).asInstanceOf[NodeHost].actorRef, propsActors(parent.get.props))
          //actorRef ! Parent(propsActors(parent.get.props))
          //println("setting Parent of", actorRef, propsActors(parent.get.props))
        }
      }
    })
    consumers.foreach(consumer => consumer.host.asInstanceOf[NodeHost].actorRef ! ChooseTentativeOperators(Seq.empty[ActorRef]))
    previousPlacement = map
  }

  def place(operator: Operator, host: Host): Unit = {
    if(host != NoHost && operator.props != null){
      val moved = previousPlacement.contains(operator) && previousPlacement(operator) != host
      if(moved) {
        propsActors(operator.props) ! Kill
        //println("killing old actor", propsActors(operator.props))
      }
      if (moved || previousPlacement.isEmpty){
        val hostActor = host.asInstanceOf[NodeHost].actorRef
        val ref = actorSystem.actorOf(operator.props.withDeploy(Deploy(scope = RemoteScope(hostActor.path.address))))
        hostActor ! SetActiveOperator(operator.props)
        hostActor ! Node(ref)
        ref ! Controller(hostActor)
        propsActors += operator.props -> ref
        //println("placing Actor", ref)
      }
    }
  }
}