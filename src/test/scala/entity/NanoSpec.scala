package com.guardian
package entity

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class NanoSpec extends AsyncWordSpec with Matchers {
  import Fixtures._

  import NanoSpec._

  "Nano.fromSecondAndNano" should {
    "return Nano" when {
      "nanosecond part is shorter than 9" in {
        Nano.fromSecondAndNano(second, badNano) shouldBe Nano(s"${second}00000$badNano")
      }
      "nanosecond part's length is 9" in {
        Nano.fromSecondAndNano(second, goodNano) shouldBe Nano(s"$second$goodNano")
      }
    }
  }
}

object NanoSpec {

  val badNano = 1234
  val goodNano = 123456789
}
