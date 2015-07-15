package kvstore

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import scala.concurrent.duration._
import akka.actor.dungeon.ReceiveTimeout
import akka.actor.ReceiveTimeout
import akka.event.LoggingReceive
import akka.actor.ActorLogging

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  
  case class SnapAckTimeout(seq: Long)

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor with ActorLogging {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
    
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  
  /* TODO Behavior for the Replicator. */
  def receive: Receive = LoggingReceive {
    case r@Replicate(key, valOp, id) => {
      val seq = nextSeq
      replica ! Snapshot(key, valOp, seq)
      acks += (seq -> (sender, r))
      context.system.scheduler.scheduleOnce(100.millis, self, SnapAckTimeout(seq))
    }
    case SnapAckTimeout(seq) => {
      if (acks contains seq) {
        val Replicate(key, valOp, _) = acks(seq)._2
        replica ! Snapshot(key, valOp, seq)
        context.system.scheduler.scheduleOnce(100.millis, self, SnapAckTimeout(seq))
      }
    }
    case SnapshotAck(key, seq) => {
      if (acks contains seq) {
        val (replyTo, Replicate(key, _, id)) = acks(seq)
        acks -= seq
        replyTo ! Replicated(key, id)
      }
    }
  }

}
