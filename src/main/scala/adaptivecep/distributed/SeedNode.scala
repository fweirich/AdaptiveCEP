package adaptivecep.distributed

import java.io.File

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory

object SeedNode{

  def main(args: Array[String]): Unit = {
    if (args.isEmpty)
      startup(Seq("application.conf"))
    else
      startup(args)
  }

  def startup(args: Seq[String]): Unit = {
      val file = new File(args.head)
      val config = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve()

      val seed: ActorSystem = ActorSystem.create("ClusterSystem", config)

      seed.actorOf(Props[SimpleClusterListener], name = "clusterListener")
  }
}
