package com.tkroman.dd.mf


import java.util.Collections
import java.util.concurrent._

class Nodes(count: Int) {
  private val nodes = Collections.newSetFromMap(
    new ConcurrentHashMap[String, java.lang.Boolean]()
  )

  def has(id: String) = nodes.contains(id)

  def count(): Int = nodes.size()

  // core logic
  def processQueue(queue: JobQueue, history: History): Unit = {
    // remove finished jobs from nodes
    val ns = nodes.iterator()
    while (ns.hasNext) {
      val doneKey = ns.next()
      val doneStatus = queue.removeNextFinished(doneKey)
      if (doneStatus != null) {
        ns.remove()
        history.add(doneKey, doneStatus)
      }
    }
    // if there is demand and free nodes, make them work
    while (nodes.size() < count && queue.hasSubmitted()) {
      nodes.add(queue.pollNextSubmitted().getValue)
    }
  }
}
