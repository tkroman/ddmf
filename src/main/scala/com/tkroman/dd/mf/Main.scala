package com.tkroman.dd.mf

import cask.model.Response
import com.typesafe.scalalogging.LazyLogging
import upickle.default._

object Main extends cask.MainRoutes with LazyLogging {
  private val ContentTypeJson =
    Seq("Content-Type" -> "application/json")

  private val numberOfNodes = sys.env("NUMBER_OF_NODES").toInt
  private val historySize = sys.env("HISTORY_SIZE").toInt
  logger.info(s"Starting with $numberOfNodes nodes & $historySize history entries")

  private val jobs: JobService =
    new QueueBasedInMemoryJobService(numberOfNodes, historySize)

  override def host: String = "0.0.0.0"

  @cask.postJson("/submitted")
  def submit(job_id: String, priority: Int): Response[String] = {
    jobs.submit(job_id, priority)
    // 201 CREATED
    cask.Response("", statusCode = 201)
  }

  @cask.postJson("/finished")
  def finish(job_id: String, status: JobStatus): Response[String] = {
    if (status != JobStatus.Failed && status != JobStatus.Succeeded) {
      // BAD REQUEST
      cask.Response("", statusCode = 400)
    } else {
      jobs.finish(job_id, status)
      // 200 OK
      cask.Response("", statusCode = 200)
    }
  }

  @cask.get("/status/:id")
  def status(id: String): Response[String] = {
    jobs.status(id) match {
      case JobStatus.Unknown =>
        cask.Response("", statusCode = 404)
      case ok =>
        cask.Response(write(Status(ok)), headers = ContentTypeJson)
    }
  }

  @cask.get("/summary")
  def summary(): Response[String] = {
    cask.Response(write(jobs.summary()), headers = ContentTypeJson)
  }

  initialize()
}
