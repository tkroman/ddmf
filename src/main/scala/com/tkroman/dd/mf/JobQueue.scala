package com.tkroman.dd.mf

import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantLock


class JobQueue {
  private val lock = new ReentrantLock(true)
  private val seq = new AtomicLong()
  private val queue = new PriorityBlockingQueue[QueuedJob](
    16,
    Comparator
      .comparingInt[QueuedJob](-_.get.priority)
      .thenComparingLong(_.seq)
  )
  private val s = new java.util.HashSet[String]()

  def enqueue(j: Job): Unit = {
    lock.lock()
    try {
      queue.add(QueuedJob(j, seq.getAndIncrement()))
      s.add(j.id)
    } finally {
      lock.unlock()
    }
  }

  def dequeue(): Option[Job] = {
    lock.lock()
    try {
      Option(queue.poll()).map { j =>
        s.remove(j.get.id)
        j.get
      }
    } finally {
      lock.unlock()
    }
  }

  def has(id: String): Boolean = {
    lock.lock()
    try {
      s.contains(id)
    } finally {
      lock.unlock()
    }
  }

  // potentially O(n) (expensive) but we expect this to happen rarely (if ever)
  // can be worked around if we keep another map of `working set` entires,
  // i.e. QueuedJob instances so that we can remove in (amortized) O(1) from the queue
  // if we switch to ConcurrentSkipListSet.
  def remove(id: String): Boolean = {
    lock.lock()
    try {
      s.remove(id)
      queue.removeIf(_.get.id == id)
    } finally {
      lock.unlock()
    }
  }

  def count() = s.size()
}
