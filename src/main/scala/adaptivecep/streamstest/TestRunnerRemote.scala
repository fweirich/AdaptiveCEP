package adaptivecep.streamstest

import java.io.File

import adaptivecep.data.Events.Event1
import adaptivecep.distributed.annealing.AppRunnerAnnealing.config
import adaptivecep.distributed.centralized.AppRunnerCentralized.{actorSystem, address1}
import adaptivecep.publishers.RandomPublisher
import akka.actor.{ActorRef, ActorSystem, Address, Deploy, Props}
import akka.remote.RemoteScope
import com.typesafe.config.{Config, ConfigFactory}

object TestRunnerRemote extends App {

  val file = new File("application.conf")
  val config: Config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()

  val actorSystem: ActorSystem = ActorSystem("ClusterSystem", config)

  val address1 = Address("akka.tcp", "ClusterSystem", "18.219.222.126", 8000)
  val address2 = Address("akka.tcp", "ClusterSystem", "3.16.193.175", 8000)

  val publisherA: ActorRef = actorSystem.actorOf(Props(RandomPublisher(id => Event1(id))).withDeploy(Deploy(scope = RemoteScope(address1))),"P")
  Thread.sleep(2000)
  val receiverA: ActorRef = actorSystem.actorOf(Props(Receiver(publisherA)).withDeploy(Deploy(scope = RemoteScope(address2))),"R")

}
