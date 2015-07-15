package calculator
import scala.math._

object Polynomial {
  def computeDelta(a: Signal[Double], b: Signal[Double],
      c: Signal[Double]): Signal[Double] = {
    Signal(b() * b() - 4 * a() * c())
  }

  def computeSolutions(a: Signal[Double], b: Signal[Double],
      c: Signal[Double], delta: Signal[Double]): Signal[Set[Double]] = {
    Signal {
      val d = delta()
      if (d < 0) {
        Set()
      } else {
        val s = sqrt(d)
        val r1 = (-b() + s) / (2 * a())
        val r2 = (-b() - s) / (2 * a())
        Set(r1, r2)
      }
    }
    
  }
}
