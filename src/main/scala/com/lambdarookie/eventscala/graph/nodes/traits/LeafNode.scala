package com.lambdarookie.eventscala.graph.nodes.traits

import com.lambdarookie.eventscala.backend.system.EventSource
import com.lambdarookie.eventscala.data.Events._
import com.lambdarookie.eventscala.graph.monitors._

trait LeafNode extends Node {

  override val operator: EventSource

  val nodeData: LeafNodeData = LeafNodeData(name, query, system, context)

}
