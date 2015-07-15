package kvstore

import akka.actor.{ OneForOneStrategy, Props, ActorRef, Actor }
import kvstore.Arbiter._
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import akka.actor.Terminated
import scala.concurrent.duration._
import akka.actor.PoisonPill
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy
import akka.util.Timeout
import akka.actor.ReceiveTimeout
import akka.event.LoggingReceive
import akka.actor.ActorLogging

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor with ActorLogging {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  
  var expectedID = 0L
  var retriesBeforeFail = 0
  
  val persistence = context.actorOf(persistenceProps, "Persistent")
  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  def nextID = {
    val ret = expectedID
    expectedID += 1
    ret
  }
  
  def replicateEntry(key:String, valOp: Option[String], nodes: Set[ActorRef]): Long = {
    val id = nextID
    nodes foreach { _ ! Replicate(key, valOp, id) }
    id
  }
  
  def membershipUpdate(replicas: Set[ActorRef]): Unit = {
    val newgroup = replicas filter { _ != self } 
    val newNodes = newgroup.diff(secondaries.keySet)
    val removedNodes = secondaries.keySet.diff(newgroup)
    removedNodes foreach { ra =>
      val rr = secondaries(ra)
      replicators -= rr
      secondaries -= ra
      context.stop(rr) 
    }
      
    var newReplicators = Set.empty[ActorRef]
    newNodes foreach  { 
      (r: ActorRef) => {
        val replicator = context.actorOf(Replicator.props(r))
        secondaries += (r -> replicator)
        newReplicators += replicator
      }
    }
    kv foreach { entry => replicateEntry(entry._1, Some(entry._2), newReplicators)}
    replicators = replicators.union(newReplicators)
  }
  
  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Insert(key, value, id) => {
      kv += (key -> value)
      val persistMsg = Persist(key, Some(value), id)
      val replyMsg = OperationAck(id)
      persistence ! persistMsg
      context setReceiveTimeout(100.milliseconds)
      context become primAwaitsPersisted(sender, persistMsg, Map.empty[Long, Set[ActorRef]])
    }
    case Remove(key, id) => {
      kv -= key
      val persistMsg = Persist(key, None, id)
      val replyMsg = OperationAck(id)
      persistence ! persistMsg
      context setReceiveTimeout(100.milliseconds)
      context become primAwaitsPersisted(sender, persistMsg, Map.empty[Long, Set[ActorRef]])
    }
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    
    case Replicas(replicas) => {
      membershipUpdate(replicas)
    }
    
  }
  
  def primAwaitsPersisted(replyTo: ActorRef, persistMsg: Persist, repAckIDs: Map[Long, Set[ActorRef]]): Receive = LoggingReceive {
     context watch persistence
     val opID = persistMsg match {
       case Persist(_, _, id) => id
     }
     
     {
        case Replicated(key, id) => {
          var remainingRepls = repAckIDs.getOrElse(id, Set.empty[ActorRef])
          remainingRepls -= sender
          remainingRepls = remainingRepls.intersect(replicators)
          if (remainingRepls.isEmpty) {
            replyTo ! OperationAck(opID)
            context become leader
          } else {
            val newRepAckIDs = repAckIDs.updated(id, remainingRepls)
            context become primAwaitsPersisted(replyTo, persistMsg, newRepAckIDs )
          }
        } 
        case Persisted(key, pid) => {
          println(s"already persisted: $pid")
          if (!replicators.isEmpty) {
            val id = replicateEntry(key, kv.get(key), replicators)
            val newRepAckIDs = repAckIDs + (id -> replicators)
            
            context become primAwaitsPersisted(replyTo, persistMsg, newRepAckIDs)
            context.system.scheduler.scheduleOnce(1.second, self, OperationFailed(opID))
          } else {
            replyTo ! OperationAck(opID)
            context become leader
          }
        }
        case Get(key, id) => {
          sender ! GetResult(key, kv.get(key), id)
        }
        case Replicas(replicas) => {
          membershipUpdate(replicas)
        }
        case ReceiveTimeout => {
          retriesBeforeFail += 1
          if (retriesBeforeFail < 10) {
            persistence ! persistMsg
            context setReceiveTimeout(100.milliseconds)
          } else {
            replyTo ! OperationFailed(opID)
            context become leader
            retriesBeforeFail = 0
          }
        }
        case Terminated(per) => {
          println("persisitence crashed!")
          replyTo ! OperationFailed(opID)
    
          context become leader
          retriesBeforeFail = 0
        }
        case of@OperationFailed(id) => {
          //println("received failed")
          val remainingRepls = repAckIDs.getOrElse(id, Set.empty[ActorRef])
          if (!remainingRepls.isEmpty) {
            replyTo ! of
          } 
          context become leader
        }
     }
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = LoggingReceive {
    case Get(key, id) => {
      sender ! GetResult(key, kv.get(key), id)
    }
    case Snapshot(key, valOp, id) => {
      if (id > expectedID) ()
      else if (id < expectedID) sender ! SnapshotAck(key, id)
      else {
        valOp match {
          case Some(value) => kv += (key -> value)
          case None        => kv -= key  
        }
        val persistMsg = Persist(key, valOp, id)
        val replyMsg = SnapshotAck(key, id)
        persistence ! persistMsg
        context become sndAwaitsPersisted(sender, persistMsg)
        context setReceiveTimeout(100.milliseconds)
      }
    }
    
  }
  
  def sndAwaitsPersisted(replyTo: ActorRef, persistMsg: Persist): Receive = LoggingReceive {
      case Persisted(key, id) => {
        replyTo ! SnapshotAck(key, id)
        expectedID += 1
        context become replica
      }
      case Get(key, id) => {
        sender ! GetResult(key, kv.get(key), id)
      }
      case ReceiveTimeout => {
        persistence ! persistMsg
        context setReceiveTimeout(100.milliseconds)
      }    
  }
}


