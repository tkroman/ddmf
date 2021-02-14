package com.tkroman.dd.mf

trait JobService {
  def submit(id: String, priority: Int): Unit
  def finish(id: String, status: JobStatus): Unit
  def status(id: String): JobStatus
  def summary(): Summary
}
