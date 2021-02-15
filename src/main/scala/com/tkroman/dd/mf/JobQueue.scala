package com.tkroman.dd.mf


import java.util.Comparator
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

// logic: keep submission/finish queues separately
// will be used by the Nodes class
// thread-safe if used from multiple thread for submissions/finishes +
// another thread for processing
class JobQueue() {
  private val in = new ConcurrentSkipListMap[(Int, Long), String](
    Comparator.comparingInt[(Int, Long)](-_._1).thenComparingLong(_._2)
  )

  private val out = new ConcurrentHashMap[String, JobStatus]()

  private val pc = new AtomicInteger()
  private val seq = new AtomicLong()

  def submissionCount(): Int = in.size()

  def hasSubmitted() = !in.isEmpty
  def hasFinished() = !out.isEmpty

  // most expensive call, could have a bidirectional map
  // but would have to pay sync costs so decided not to go with that
  def hasSubmission(id: String) = in.containsValue(id)

  def submit(id: String, prio: Int): Unit = {
    in.put(prio -> seq.getAndIncrement(), id)
    pc.incrementAndGet()
  }

  def pollNextSubmitted() = {
    pc.decrementAndGet()
    in.pollFirstEntry()
  }

  def removeNextFinished(id: String) = {
    out.remove(id)
  }

  def finish(id: String, status: JobStatus): Unit = {
    out.put(id, status)
  }
}
