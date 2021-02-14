package com.tkroman.dd.mf

import upickle.default._

case class Status(status: JobStatus)
object Status {
  implicit val rw: ReadWriter[Status] = macroRW[Status]
}

case class Summary(pending: Int, running: Int, succeeded: Int, failed: Int)
object Summary {
  implicit val rw: ReadWriter[Summary] = macroRW[Summary]
}

sealed trait JobStatus
object JobStatus {
  case object Pending extends JobStatus
  case object Running extends JobStatus
  case object Succeeded extends JobStatus
  case object Failed extends JobStatus
  case object Unknown extends JobStatus

  private val httpNames: Map[String, JobStatus] = Map(
    "PENDING" -> Pending,
    "RUNNING" -> Running,
    "SUCCEEDED" -> Succeeded,
    "FAILED" -> Failed,
    "UNKNOWN" -> Unknown,
  )

  private val reverseHttpNames: Map[JobStatus, String] =
    httpNames.toList.map(_.swap).toMap

  implicit def rw: upickle.default.ReadWriter[JobStatus] =
    upickle.default.readwriter[String].bimap[JobStatus](
      reverseHttpNames(_),
      httpNames(_)
    )
}

case class Job(id: String, priority: Int, status: JobStatus)
case class QueuedJob(get: Job, seq: Long)
