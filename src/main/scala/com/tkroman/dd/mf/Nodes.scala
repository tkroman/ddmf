package com.tkroman.dd.mf

import java.util.concurrent.Semaphore

// simple bounded concurrent hash set of objects to keep track of which jobs
// are considered to be `JobStatus.Running` now.
// all unspecified and/or undefined state/behavior is throwing ISE
// so that we would fail fast on these requests instead of ending up
// in an inconsistent state
class Nodes(count: Int) {
  // this is responsible for bounding the working set size
  private val l = new Semaphore(count)
  // concurrent hash set :)
  private val o = new Object
  private val m = new java.util.concurrent.ConcurrentHashMap[String, Object]()

  def count(): Int = m.size()

  def has(id: String): Boolean = m.containsKey(id)

  def offerTaskBlocking(id: String): Unit = {
    l.acquire()
    add(id)
    verifyBoundedness()
  }

  def offerTask(id: String): Boolean = {
    val accepted = l.tryAcquire()
    if (accepted) {
      add(id)
    }
    verifyBoundedness()
    accepted
  }

  private def verifyBoundedness() = {
    val sz = m.size()
    if (sz > count) {
      throw new IllegalStateException(
        s"Invariant failed: node count = $sz, limit = $count (shouldn't happen)"
      )
    }
  }

  private def add(id: String) = {
    if (m.putIfAbsent(id, o) != null) {
      throw new IllegalStateException(s"Task $id is already being handled")
    }
  }

  // should only be called when we are certain there is a free node
  def submitTask(id: String): Unit = {
    if (!offerTask(id)) {
      // shouldn't happen when this method is used according to its contract
      throw new IllegalStateException(s"Couldn't submit task $id")
    }
  }

  def evictTask(id: String): Boolean = {
    val removed = m.remove(id) != null
    if (removed) {
      l.release()
    }
    removed
  }
}
