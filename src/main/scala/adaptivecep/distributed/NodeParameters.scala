package adaptivecep.distributed

import scala.concurrent.duration.Duration

class NodeParameters(var latency: Duration, var bandwidth: Double) extends Comparable[NodeParameters] {

  def getLatency: Duration = latency

  def setLatency(latency: Duration): Unit = {
    this.latency = latency
  }
  def getBandwidth: Double = bandwidth

  def setBandwidth(bandwidth: Double): Unit = {
    this.bandwidth = bandwidth
  }

  override def compareTo(other: NodeParameters): Int = {
    val d0 = -this.getLatency.toMillis
    val d1 = -other.getLatency.toMillis
    val n0 = this.getBandwidth
    val n1 = other.getBandwidth
    if ((d0 == d1) && (n0 == n1)) 0
    else if (d0 < d1 && n0 < n1) -1
    else if (d0 > d1 && n0 > n1) 1
    else Math.signum((d0 - d1) / Math.abs(d0 + d1) + (n0 - n1) / Math.abs(n0 + n1)).toInt
  }

}