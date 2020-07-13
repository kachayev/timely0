package timely0

import castor.{SimpleActor, Context}

import scala.collection.mutable.Map
import scala.collection.immutable.{Vector, Set}
import scala.math.Ordered
import scala.language.implicitConversions

import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

object Dataflow {

  import Context.Simple._

  // xxx(okachaiev): reimplement Time being semantically closer to the paper
  sealed trait Time extends Product with Serializable with Ordered[Time] {
    def compare(that: Time): Int
  }

  final case class TimeCell(stage: Int, at: Int)

  object Time {
    case object Zero extends Time {
      def compare(that: Time) = that match {
        case Zero => 0
        case _ => -1
      }
    }

    case class LoopCounter(head: TimeCell, tail: Time) extends Time {
      def compare(that: Time) = that match {
        case Zero => 1
        case LoopCounter(TimeCell(stage, at), thatTail) =>
          if (head.stage == stage) head.at.compare(at)
          else if (head.stage < stage) this.compare(thatTail)
          else tail.compare(that)
      }
    }

    val Epoch = Time(0)

    def apply(at: Int): Time =
      LoopCounter(TimeCell(1, at), Zero)

    def max(t1: Time, t2: Time): Time =
      if (t1.compare(t2) < 0) t2 else t1

    def advance(t1: Time): Time = t1 match {
      case LoopCounter(TimeCell(stage, at), tail) => LoopCounter(TimeCell(stage, at+1), tail) 
      case Zero => Zero
    }

    def branch(t1: Time): Time = t1 match {
      case Zero => Time(0)
      case LoopCounter(TimeCell(stage, at), tail) =>
        LoopCounter(TimeCell(stage+1, 0), LoopCounter(TimeCell(stage, at), tail))
    }

    def debranch(t1: Time): Time = t1 match {
      case Zero => Zero
      case LoopCounter(_, tail) => tail
    }
  }

  implicit def int2Time(x: Int): Time = Time(x)

  type VertexId = Int
  
  final case class Message[T](edge: Edge, payload: T, time: Time)
  
  type Vertex[T] = SimpleActor[Message[T]]
  type Notification = Time
  type Notify = SimpleActor[Notification]
  type Dataflow = Map[VertexId, List[VertexId]]

  case class Edge(source: VertexId, target: VertexId)
  case class Pointstamp(time: Time, id: VertexId)

  sealed trait ProgressAction

  object ProgressAction {
    case class Update(vertex: VertexId, at: Time) extends ProgressAction
    case class IncrementOccurence(point: Pointstamp) extends ProgressAction
    case class DecrementOccurence(point: Pointstamp) extends ProgressAction
  }

  sealed trait NotifyAction

  object NotifyAction {
    case class At(vertex: VertexId, at: Time) extends NotifyAction
    case class Send(point: Pointstamp) extends NotifyAction 
  }

  // Naiad has 2 separate concepts: Controller and Computation
  // here we are going to simplify to a single notion of "Computation"
  // that encapsulates dataflow graph, progress tracker, and scheduler
  // as far as the program runs in a single process only, there is no
  // need to deal with workers, sync, join handles etc
  class Computation {

    val index = new AtomicInteger(0)
    val currentRound: Map[VertexId, Time] = Map.empty[VertexId, Time]
    val refs: Map[VertexId, Vertex[_]] = Map.empty[VertexId, Vertex[_]]
    val graph: Dataflow = Map.empty[VertexId, List[VertexId]]
    val reverseGraph: Dataflow = Map.empty[VertexId, List[VertexId]]
    val notifyRefs: Map[VertexId, Notify] = Map.empty[VertexId, Notify]
    val occurence: ConcurrentMap[Pointstamp, Int] = new ConcurrentHashMap[Pointstamp, Int]
   
    val progressTracker = new SimpleActor[ProgressAction]() {
      override def run(message: ProgressAction) = message match {
        case ProgressAction.Update(vertex, at) => {
          val round = currentRound.get(vertex)
          // assuming "reachableFrom" also includes itself
          val reachable = reachableFrom(vertex)
          reachable foreach { edge =>
            currentRound.put(edge, Time.max(at, currentRound.get(edge).getOrElse(Time.Epoch)))
          }
          round.map({ at =>
            reachable foreach { edge => notifier.send(NotifyAction.Send(Pointstamp(at, edge))) }
          })
        }

        case ProgressAction.IncrementOccurence(point: Pointstamp) => 
          occurence.compute(point, (_, counter) => {
            if (counter.equals(null)) 1
            else counter + 1
          })

        case ProgressAction.DecrementOccurence(point: Pointstamp) =>
          occurence.computeIfPresent(point, (_, counter) => {
            val newCounter = counter-1
            if (newCounter == 0) notifier.send(NotifyAction.Send(point))
            newCounter
          })
      }
    }

    val notifier = new SimpleActor[NotifyAction]() {
      val notifications: Map[Time, Set[VertexId]] = Map.empty[Time, Set[VertexId]]
      
      def shouldNotify(at: Time, vertex: VertexId): Boolean =
        notifications.get(at) match {
          case Some(edges) => edges.contains(vertex)
          case None => false
        }

      def notify(at: Time, vertex: VertexId) = {
        val targets = notifications.getOrElse(at, Set.empty[VertexId])
        notifications.put(at, targets-vertex)
        notifyRefs.get(vertex).map(_.send(at))
      }

      override def run(message: NotifyAction) = message match {
        // called when # of message for a specific Vertex at specific Time
        // is dropped to 0. in case if all "previous" nodes in a graph
        // have current round > notification time, we should fire
        // notification
        case NotifyAction.Send(Pointstamp(at, vertex)) =>
          if (shouldNotify(at, vertex)) {
            val notifiable = reachableTo(vertex) forall { prev =>
              currentRound.get(prev) match {
                case Some(round) => round > at && occurence.get((prev, at), 0) == 0
                case None => true // should not happen, right?
              }
            }
            if (notifiable) this.notify(at, vertex)
          }
        case NotifyAction.At(vertex: VertexId, at: Time) => {
          val targets = notifications.getOrElse(at, Set.empty[VertexId])
          notifications.put(at, targets + vertex)
        }
      }
    }

    def newIndex(): Int = index.incrementAndGet()
    
    def registerVertexRef(id: VertexId, ref: Vertex[_]) = refs.put(id, ref)

    def registerEdgeIn(graph: Dataflow, source: VertexId, target: VertexId) = {
      val v = graph.getOrElse(source, Nil)
      graph.put(source, target :: v)
    }

    def registerEdge(source: VertexId, target: VertexId) = {
      registerEdgeIn(graph, source, target)
      registerEdgeIn(reverseGraph, target, source)
    }

    def registerNotifyRef(source: VertexId, target: Notify) = notifyRefs.put(source, target)

    def notifyAt(vertex: VertexId, at: Time) = notifier.send(NotifyAction.At(vertex, at))

    def send[T](message: Message[T]) =
      refs.get(message.edge.target).map({ target =>
        val point = Pointstamp(message.time, message.edge.target)
        progressTracker.send(ProgressAction.IncrementOccurence(point))
        target.asInstanceOf[Vertex[T]].send(message)
      })

    // xxx(okachaiev): update reachability when adding vertex/edges
    // not each time we send a message
    // xxx(okachaiev): extremely inefficient way to do a traversal
    def recomputeReachability(graph: Dataflow, vertex: VertexId): Set[VertexId] = {
      def bfs(state: Set[VertexId]): Set[VertexId] = {
        val newState = state.foldLeft(Set.empty[VertexId])({ (cursor, node) =>
          val targets = graph.getOrElse(node, List.empty[VertexId]).toSet
          cursor ++ targets
        })
        if (newState == state) state
        else bfs(newState)
      }

      bfs(Set(vertex))
    }

    def reachableTo(vertex: VertexId): Set[VertexId] =
      recomputeReachability(reverseGraph, vertex) - vertex

    def reachableFrom(vertex: VertexId): Set[VertexId] =
      recomputeReachability(graph, vertex)

    def broadcastProgressUpdate(vertex: VertexId, at: Time) =
      progressTracker.send(ProgressAction.Update(vertex, at))

    def decrementOccurence(point: Pointstamp) =
      progressTracker.send(ProgressAction.DecrementOccurence(point))

    def newInput[T](): Input[T] = new Input[T](this)

    def newOutput(source: VertexId): Edge = {
      val target = newIndex()
      registerEdge(source, target)
      Edge(source, target)
    }

    def subscribe[T](source: Edge, callback: T => Unit): Unit =
      new Subscription(this, source, callback)
  }

  class Input[T](df: Computation) {
    val refId = df.newIndex()
    val output = df.newOutput(refId)
    val isCompleted = new AtomicBoolean(false)
    // relying on the fact that input cannot be a part of
    // any loops :thinking:
    val currentEpoch = new AtomicInteger(0)

    val ref = new SimpleActor[T]() {
      override def run(payload: T) = {
        val currentTime = Time(currentEpoch.getAndIncrement())
        df.send(Message(output, payload, currentTime))
        df.broadcastProgressUpdate(refId, currentTime)
      }
    }

    def onNext(payload: T) = if (!isCompleted.get) ref.send(payload)
    def onComplete() = isCompleted.compareAndSet(false, true)
  }

  // it would be much better to have "sendBy" and "notifyAt"
  // as functions of computation/controller not functions of the vertex
  // itself to avoid coupling between vertex implementation and
  // very specific computation/controller they are intended to be used for
  abstract class UnaryVertex[T](df: Computation, source: Edge) {

    val refId = source.target
    
    def onRecv(edge: Edge, msg: T, time: Time)
    def onNotify(at: Time) = {}
    
    df.registerVertexRef(refId, new SimpleActor[Message[T]]() {
      override def run(message: Message[T]) = {
        onRecv(message.edge, message.payload, message.time)
        df.decrementOccurence(Pointstamp(message.time, refId))
      }
    })

    df.registerNotifyRef(refId, new SimpleActor[Notification]() {
      override def run(message: Notification) = onNotify(message)
    })

    val output = df.newOutput(refId)

    def sendBy[T](edge: Edge, msg: T, time: Time) = df.send(Message(edge, msg, time))
    def notifyAt(at: Time) = df.notifyAt(refId, at)
  }

  class Subscription[T](df: Computation, source: Edge, callback: T => Unit)
    extends UnaryVertex[T](df, source) {
    def onRecv(e: Edge, msg: T, time: Time) = callback(msg)
  }

  // LoopContext provides 2 different "output" edges to work with:
  // * this.feedback (to loop the message back into the loop)
  // * this.egress (to send message to an outer scope)
  // ingress is automatically linked (bounded) to the source edge
  // by redefining UnaryVertex constructor parameters
  abstract class LoopContext[T, E](df: Computation, source: Edge)
    extends UnaryVertex[T](df, Edge(source.source, df.newIndex())) {

    class Rerouter[T](e: Edge, timer: Time => Time) extends SimpleActor[Message[T]] {
      override def run(message: Message[T]) = message match {
        case Message(_edge, payload, time) =>
          df.send(Message(e, payload, timer(time)))
      }
    }

    val ingressId = source.target
    val ingress2vertex = Edge(ingressId, refId)
    val feedback = df.newOutput(this.refId)
    val feedback2vertex = Edge(feedback.target, refId)
    val egress = df.newOutput(this.refId)
    val egress2output = Edge(egress.target, output.target)

    df.registerVertexRef(ingressId, new Rerouter[T](ingress2vertex, Time.branch))
    df.registerVertexRef(feedback.target, new Rerouter[T](feedback2vertex, Time.advance))
    df.registerVertexRef(egress.target, new Rerouter[E](egress2output, Time.debranch))    
    df.registerEdge(ingressId, refId)
    df.registerEdge(feedback.target, refId)
  }
}
