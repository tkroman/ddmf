package com.tkroman.dd.mf

import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger


class History(size: Int) {
  // FIXME: should use sth like Caffeine for a proper
  //  LRU/size-bounded map implementation in production
  private val h = Collections.synchronizedMap(
    new java.util.LinkedHashMap[String, JobStatus]() {
      override def removeEldestEntry(eldest: java.util.Map.Entry[String, JobStatus]): Boolean = {
        size() > History.this.size
      }
    }
  )
  private val failedCount = new AtomicInteger()
  private val succeededCount = new AtomicInteger()

  def add(id: String, status: JobStatus): Unit = {
    val prev = h.put(id, status)
    // turn this into HTTP 500 since we don't know how we ended up here
    if (prev != null) {
      throw new IllegalStateException(
        s"job $id ($status) is already present in history"
      )
    }
    if (status == JobStatus.Failed) {
      failedCount.incrementAndGet()
    } else if (status == JobStatus.Succeeded) {
      succeededCount.incrementAndGet()
    }
  }

  def get(id: String): Option[JobStatus] = {
    Option(h.get(id))
  }

  def failed(): Int = failedCount.get()
  def succeeded(): Int = succeededCount.get()
}
