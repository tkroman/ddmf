package com.tkroman.dd.mf

import java.util.concurrent.locks.ReentrantLock

class LockingInMemoryJobService(nodeCount: Int,
                                historySize: Int) extends JobService {
  // in real-life scenarios these fields
  // could be passed as constructor parameters (DI)
  // to improve testability etc but we don't care for now
  // and refactoring won't be too hard
  private val nodes = new Nodes(nodeCount)
  private val queue = new JobQueue()
  private val history = new History(historySize)

  // each dependency/component is designed to be thread-safe
  // but we still need synchronization between them
  private val syncNodesAndQueue = new ReentrantLock(true)

  // dispatches to a free node or enqueues for futher processing
  // NOTE: another viable approach would be to put into a queue
  // straight away and have a background thread that tries to transfer
  // from the queue to nodes 1-by-1.
  // Probably would have even better performance due to being lock-free
  // but we would have to measure and compare.
  // But even in that case we would have to synchronize on reads/writes
  // between submission and finishes.
  // ===================================
  // Algorithm in that case would be:
  // IN := priority queue (same as in JobQueue now)
  // OUT := ConcurrentMap[String, JobStatus]
  // nodes := (concurrent set + a semaphore to keep track of size bounds)
  // submit := IN += job
  // finish := OUT += (id -> status)
  // schedule a function to run e.g. every 1-5ms:
  //  for o in OUT:
  //    if (nodes.remove(o.id) || in.remove(o.id)) history += o.id -> o.status
  //  while (nodes.offer(in.id)) {}
  //  With this implemented, submit/finish become O(1) at a cost of
  //  having 2 theoretically unbounded queues but in practice they would be
  //  bounded by the natural rate of submit->finish cycle.
  override def submit(id: String, priority: Int): Unit = {
    val rj = Job(id, priority, JobStatus.Running)
    // sync b/c we don't want races between
    syncNodesAndQueue.lock()
    try {
      if (!nodes.offerTask(rj.id)) {
        queue.enqueue(rj.copy(status = JobStatus.Pending))
      }
    } finally {
      syncNodesAndQueue.unlock()
    }
  }

  override def finish(id: String, status: JobStatus): Unit = {
    syncNodesAndQueue.lock()
    try {
      // happy path
      val evictedFromNodes = nodes.evictTask(id)
      // rare case when job was marked as finished before it was submitted to us for the monitoring
      // (IDK when this should happen but what if client requests come out of order?)
      val evictedFromQueue = queue.remove(id)
      if (evictedFromNodes || evictedFromQueue) {
        history.add(id, status)
        queue.dequeue().foreach(t => nodes.submitTask(t.id))
      } else {
        throw new IllegalStateException(s"Task $id is not in the working set")
      }
    } finally {
      syncNodesAndQueue.unlock()
    }
  }

  override def status(id: String): JobStatus = {
    history.get(id) match {
      case Some(s) => s
      case None if nodes.has(id) => JobStatus.Running
      case None if queue.has(id) => JobStatus.Pending
      case _ => JobStatus.Unknown
    }
  }

  override def summary(): Summary = {
    // have to sync to keep invariant (queue.count() + nodes.count() == #sumbitted)
    // otherwise we risk inconsistent results b/w reads on `queue` & `nodes`
    syncNodesAndQueue.lock()
    try {
      Summary(
        queue.count(),
        nodes.count(),
        history.succeeded(),
        history.failed()
      )
    } finally {
      syncNodesAndQueue.unlock()
    }
  }
}
