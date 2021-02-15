package com.tkroman.dd.mf

import com.typesafe.scalalogging.LazyLogging

import java.util.concurrent._
import scala.util.chaining._
import scala.util.control.NonFatal

class QueueBasedInMemoryJobService(nodeCount: Int,
                                   historySize: Int) extends JobService with LazyLogging {
  private val history = new History(historySize)
  private val queue = new JobQueue()
  private val nodes = new Nodes(nodeCount)

  private val exceptionLoggingThreadFactory: ThreadFactory =
    (r: Runnable) => new Thread(r).tap {
      _.setUncaughtExceptionHandler((t: Thread, e: Throwable) =>
        logger.error(s"Error in thread $t", e))
    }

  private val ec = Executors
    // use single thread for processing which is going to be fast
    // since we are basically consuming from queues w/o contention
    // and can write from arbitrary number of threads concurrently
    .newSingleThreadScheduledExecutor(exceptionLoggingThreadFactory)
    .tap(_.scheduleWithFixedDelay(() => {
      // process in/out queues every slice of time
      try {
        step()
      } catch {
        case NonFatal(e) =>
          logger.error("Unexpected error", e)
        case other =>
          logger.error("Unexpected failure", other)
          throw other
      }
    }, 0, 10, TimeUnit.MICROSECONDS)) // FIXME config

  override def submit(id: String, priority: Int): Unit = {
    // work sill goes into this queue even though it's kind of
    // against requirements. in my defence, since queue is priority based,
    // if a hi-prio job comes in, it will be put into the next available node
    // almost instantly since we schedule the `step()` (consumer function)
    // to run really frequently
    // but this way we can have only 1 place where the updates are happening
    queue.submit(id, priority)
  }

  override def finish(id: String, status: JobStatus): Unit = {
    queue.finish(id, status)
  }

  private def step(): Unit = {
    nodes.processQueue(queue, history)
  }

  override def status(id: String): JobStatus = {
    Option(history.get(id)) match {
      case Some(s) => s
      case None if nodes.has(id) => JobStatus.Running
      case None if queue.hasSubmission(id) => JobStatus.Pending
      case _ => JobStatus.Unknown
    }
  }

  override def summary(): Summary = {
    Summary(
      queue.submissionCount(),
      nodes.count(),
      history.succeeded(),
      history.failed(),
    )
  }

  def stop() = {
    try {
      ec.shutdown()
      if (!ec.awaitTermination(5, TimeUnit.SECONDS)) {
        ec.shutdownNow()
      }
    } catch {
      case NonFatal(e) => logger.error("Failed to stop", e)
    }
  }
}
