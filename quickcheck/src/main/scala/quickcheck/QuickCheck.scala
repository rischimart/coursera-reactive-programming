package quickcheck

import common._
import org.scalacheck._
import Arbitrary._
import Gen._
import Prop._
import scala.math._

abstract class QuickCheckHeap extends Properties("Heap") with IntHeap {
  def heapSort(h : H, acc : List[Int]) : List[Int] = {
    if (isEmpty(h)) acc.reverse
    else {
      val m = findMin(h)
      heapSort(deleteMin(h), m :: acc)
    }
  }

  property("min1") = forAll { a: Int =>
    val h = insert(a, empty)
    findMin(h) == a
  }
  
  property("insert2elems") = forAll { (a : Int, b : Int) =>
    val h = insert(b, insert(a, empty))
    findMin(h) == min(a, b)
  }
  
  property("insert1delete1") = forAll { a : Int =>
    val h = deleteMin(insert(a, empty))
    isEmpty(h)
  }
  
  property("heapsort") = forAll { a : H => 
    val l = heapSort(a, List())
    l == l.sorted
  }
  
  property("minOf2Heaps") = forAll { (a: H, b : H) => 
    findMin(meld(a, b)) == min(findMin(a), findMin(b))
  }
  
  def merge(a : List[Int], b: List[Int]) : List[Int] = {
    (a, b) match {
      case (Nil, x) => x
      case (x, Nil) => x
      case (aa :: as, bb :: bs) => {
        if (aa <= bb) aa :: merge(as, b) 
        else          bb :: merge(a, bs)  
      }
    }
  }
    
  property("linkbehavior") = forAll { (a: H, b : H) => 
    val s = heapSort(meld(a, b), List())
    s == merge(heapSort(a, List()), heapSort(b, List()))
  }

  lazy val genHeap: Gen[H] = for {
    v <- arbitrary[Int]
    h <- oneOf(const(empty), genHeap)
  } yield insert(v, h)
  
  implicit lazy val arbHeap: Arbitrary[H] = Arbitrary(genHeap)

}
