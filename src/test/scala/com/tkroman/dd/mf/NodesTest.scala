package com.tkroman.dd.mf

import utest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, blocking}

object NodesTest extends TestSuite {
  override def tests: Tests = Tests {
    test("basic case, happy path") {
      val ns = new Nodes(4)

      val offered = (1 to 8).map(i => ns.offerTask(s"$i"))
      assert(offered.count(_ == true) == 4)
      assert(offered.count(_ == false) == 4)

      val evicted = (1 to 4).map(i => ns.evictTask(s"$i"))
      assert(evicted.count(_ == true) == 4)

      // if doesn't throw we are fine
      (1 to 4).foreach(i => ns.submitTask(s"$i"))

      val msg = intercept[IllegalStateException](ns.submitTask("5")).getMessage
      assert(msg == "Couldn't submit task 5")
    }

    test("parallel submission") {
      val ns = new Nodes(4)
      import ExecutionContext.Implicits.global
      val rf = Future.traverse((1 to 1024).toSet) { i =>
        Future {
          blocking {
            val id = s"$i"
            while (!ns.offerTask(id)) {
              Thread.sleep(10)
            }
            while (!ns.evictTask(id)) {
              Thread.sleep(10)
            }
          }
          i
        }
      }
      val r = Await.result(rf, Duration.Inf)
      assert((1 to 1024).toSet == r)
    }
  }
}
