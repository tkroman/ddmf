package com.tkroman.dd.mf

import com.tkroman.dd.mf.JobStatus.{Failed, Succeeded}
import utest._

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, blocking}

object HistoryTest extends TestSuite {
  override def tests: Tests = Tests {
    test("happy path") {
      val h = new History(4)
      h.add("0", Succeeded)
      h.add("1", Succeeded)
      h.add("2", Succeeded)
      h.add("3", Failed)
      h.add("4", Failed)
      h.add("5", Failed)
      h.add("6", Failed)
      h.add("7", Succeeded)
      assert(
        h.succeeded() == 4,
        h.failed() == 4
      )
      assert(
        h.get("0").isEmpty,
        h.get("1").isEmpty,
        h.get("2").isEmpty,
        h.get("3").isEmpty,
        h.get("4").contains(Failed),
        h.get("5").contains(Failed),
        h.get("6").contains(Failed),
        h.get("7").contains(Succeeded),
      )
    }

    test("concurrent load") {
      val h = new History(512)
      import scala.concurrent.ExecutionContext.Implicits.global
      val r = Future.traverse((0 until 1024).toList) { i =>
        Future {
          blocking {
            h.add(s"$i", if (i % 2 == 0) Succeeded else Failed)
          }
        }
      }
      Await.result(r, Duration(5, TimeUnit.SECONDS))
      assert(h.failed() == 512)
      assert(h.succeeded() == 512)

      val present = (0 until 1024).flatMap(i => h.get(s"$i"))
      assert(present.size == 512)

      // can't make more assertions since order is undefined
      // so we don't know how much of which kind is currently stored
    }
  }
}
