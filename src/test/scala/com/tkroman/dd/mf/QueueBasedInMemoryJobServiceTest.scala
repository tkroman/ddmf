package com.tkroman.dd.mf

import com.tkroman.dd.mf.JobStatus.{Failed, Pending, Succeeded}
import utest._

import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.util.Random

object QueueBasedInMemoryJobServiceTest extends TestSuite {
  override def tests: Tests = Tests {
    test("free node, empty queue") {
      val s = new QueueBasedInMemoryJobService(1, 4)
      val j1 = "1"
      s.submit(j1, 1)
      eventually(s.status(j1) == JobStatus.Running)

      val j2 = "2"
      s.submit(j2, 10)
      eventually(s.status(j2) == JobStatus.Pending)
      eventually(s.status(j1) == JobStatus.Running)

      val j3 = "3"
      s.submit(j3, 10)
      eventually(s.status(j3) == JobStatus.Pending)
      eventually(s.status(j2) == JobStatus.Pending)
      eventually(s.status(j1) == JobStatus.Running)

      val j4 = "4"
      s.submit(j4, 15)
      eventually(s.status(j4) == JobStatus.Pending)
      eventually(s.status(j3) == JobStatus.Pending)
      eventually(s.status(j2) == JobStatus.Pending)
      eventually(s.status(j1) == JobStatus.Running)

      s.finish(j1, JobStatus.Succeeded)
      eventually(s.status(j4) == JobStatus.Running)
      eventually(s.status(j3) == JobStatus.Pending)
      eventually(s.status(j2) == JobStatus.Pending)
      eventually(s.status(j1) == JobStatus.Succeeded)

      s.finish(j4, JobStatus.Succeeded)
      eventually(s.status(j4) == JobStatus.Succeeded)
      eventually(s.status(j3) == JobStatus.Pending)
      eventually(s.status(j2) == JobStatus.Running)
      eventually(s.status(j1) == JobStatus.Succeeded)

      s.finish(j2, JobStatus.Succeeded)
      eventually(s.status(j4) == JobStatus.Succeeded)
      eventually(s.status(j3) == JobStatus.Running)
      eventually(s.status(j2) == JobStatus.Succeeded)
      eventually(s.status(j1) == JobStatus.Succeeded)

      s.finish(j3, JobStatus.Succeeded)
      eventually(s.status(j4) == JobStatus.Succeeded)
      eventually(s.status(j3) == JobStatus.Succeeded)
      eventually(s.status(j2) == JobStatus.Succeeded)
      eventually(s.status(j1) == JobStatus.Succeeded)
    }
    test("concurrent submission") {
      import scala.concurrent.ExecutionContext.Implicits.global
      val svc = new QueueBasedInMemoryJobService(4, 1024)
      val total = 16 * 1024
      val ids = (0 until total).toList
      val q = new LinkedBlockingQueue[Integer](total)
      val s = Future.traverse(ids) { i =>
        val id = s"$i"
        Future { svc.submit(id, Random.nextInt(25)); q.put(i) }
      }.map(_ => ())
      val qq = new LinkedBlockingQueue[Integer](ids.map(_.asInstanceOf[Integer]).asJava)
      val r = Future {
        while (!q.isEmpty) {
          val i = q.poll()
          if (svc.status(s"$i") != Pending) {
            qq.offer(i)
          } else {
            q.put(i)
          }
        }
      }
      val f = Future {
        var cnt = 0
        while (cnt < total) {
          val el = qq.poll()
          if (el != null) {
            svc.finish(s"$el", if (el % 2 == 0) Succeeded else Failed)
            cnt += 1
          }
        }
      }
      val awaitAllFinished = Future {
        val expected = Summary(0, 0, 8 * 1024, 8 * 1024)
        while (svc.summary() != expected) {
          Thread.sleep(10)
        }
      }
      Await.result(s, Duration.Inf)
      Await.result(r, Duration.Inf)
      Await.result(f, Duration.Inf)
      Await.result(awaitAllFinished, Duration.Inf)
    }
  }
}
