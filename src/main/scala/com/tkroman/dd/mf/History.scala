package com.tkroman.dd.mf


import com.tkroman.dd.mf.JobStatus.{Failed, Succeeded}

import java.util
import java.util.concurrent.atomic.AtomicInteger

// logic: since we handle this in a single thread,
// we can avoid synchronization on the map
class History(capacity: Int) {
  private val history: java.util.Map[String, JobStatus] =
    new util.LinkedHashMap[String, JobStatus]() {
      override def removeEldestEntry(eldest: java.util.Map.Entry[String, JobStatus]): Boolean = {
        // evict if capacity exceeded
        size() > capacity
      }
    }
  private val failedCount = new AtomicInteger()
  private val succeededCount = new AtomicInteger()

  def add(id: String, s: JobStatus): Unit = {
    if (s == Succeeded) {
      succeededCount.incrementAndGet()
    }
    if (s == Failed) {
      failedCount.incrementAndGet()
    }
    history.put(id, s)
  }
  def get(id: String) = history.get(id)
  def succeeded(): Int = succeededCount.get()
  def failed(): Int = failedCount.get()
}
