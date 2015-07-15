/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue
import akka.event.LoggingReceive

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply


}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  
  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = { 
    case Insert(requester, id, elem) => root ! Insert(requester, id, elem)
    case Contains(requester, id, elem) => root ! Contains(requester, id, elem)
    case Remove(requester, id, elem) => root ! Remove(requester, id, elem)
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become garbageCollecting(newRoot)
    }
  }
  
  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case i@Insert(_, id, elem)  => pendingQueue = pendingQueue.enqueue(i)
    case c@Contains(_, id, elem) => pendingQueue = pendingQueue.enqueue(c)
    case r@Remove(_, id, elem)  => pendingQueue = pendingQueue.enqueue(r)
    case CopyFinished => {
      pendingQueue foreach (newRoot ! _)
      pendingQueue = Queue.empty[Operation]
      root = newRoot
      context unbecome()
    }
  }
}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor  {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved
  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(requester, id, insertElem) => {
      if (insertElem < elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! Insert(requester, id, insertElem)
          case None       => {
            val leftTree = context.actorOf(BinaryTreeNode.props(insertElem, initiallyRemoved = false))
            subtrees += (Left -> leftTree)
            requester ! OperationFinished(id)
          }
        }
      } else if (insertElem > elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! Insert(requester, id, insertElem)
          case None       => {
            val rightTree = context.actorOf(BinaryTreeNode.props(insertElem, initiallyRemoved = false))
            subtrees += (Right -> rightTree)
            requester ! OperationFinished(id)
          }
        }
      } else {
        if (removed) removed = false
        requester ! OperationFinished(id)
      }
    }
    case Contains(requester, id, insertElem) => {
      if (insertElem < elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! Contains(requester, id, insertElem)
          case None       => requester ! ContainsResult(id, false)
        }
      } else if (insertElem > elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! Contains(requester, id, insertElem)
          case None       => requester ! ContainsResult(id, false)
        }
      } else {
        requester ! ContainsResult(id, !removed)
      }
    }
    case Remove(requester, id, insertElem) => {
      if (insertElem < elem) {
        subtrees.get(Left) match {
          case Some(left) => left ! Remove(requester, id, insertElem)
          case None       => requester ! OperationFinished(id)
        }
      } else if (insertElem > elem) {
        subtrees.get(Right) match {
          case Some(right) => right ! Remove(requester, id, insertElem)
          case None       => requester ! OperationFinished(id)
        }
      } else {
        removed = true
        requester ! OperationFinished(id)
      }
    }
    case CopyTo(newRoot) => {
      context become copying(subtrees.values.toSet, removed)
      if (!removed) {
        newRoot ! Insert(self, elem, elem)
      } else if (subtrees.isEmpty) {
        context.parent ! CopyFinished
        context.stop(self)
      }
      subtrees.values foreach (_ ! CopyTo(newRoot))
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => {
       if (expected.isEmpty) {
         context.parent ! CopyFinished
         context.stop(self)
       } else {
         context become copying(expected, true)
       }
    }
    case CopyFinished => {
       val newExpected = expected - sender
       if (newExpected.isEmpty && insertConfirmed) {
         context.parent ! CopyFinished
         context.stop(self)
       } else {
         context become copying(newExpected, insertConfirmed)
       }
    }
  }


}
