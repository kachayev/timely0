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

  final case class Message[T](payload: T, time: Time)
  
  type Vertex[T] = SimpleActor[(VertexId, Message[T])]

  type VertexId = Int

  type Notify = Time

  type Dataflow = Map[VertexId, List[VertexId]]

  case class Edge(source: VertexId, target: VertexId)

  case class Pointstamp(time: Time, id: VertexId)

  // Naiad has 2 separate concepts: Controller and Computation
  // here we are going to simplify to a single notion of "Computation"
  // that encapsulates dataflow graph, progress tracker, and scheduler
  // as far as the program runs in a single process only, there is no
  // need to deal with workers, sync, join handles etc
  class Computation {

    val index = new AtomicInteger(0)

    val currentRound: Map[VertexId, Time] = Map.empty[VertexId, Time]

    val edges: Map[VertexId, Vertex[_]] = Map.empty[VertexId, Vertex[_]]

    val graph: Dataflow = Map.empty[VertexId, List[VertexId]]

    val reverseGraph: Dataflow = Map.empty[VertexId, List[VertexId]]

    val notifications: Map[Time, Set[VertexId]] = Map.empty[Time, Set[VertexId]]

    val notifyVertex: Map[VertexId, Vertex[Notify]] = Map.empty[VertexId, Vertex[Notify]]
    
    val occurence: ConcurrentMap[Pointstamp, Int] = new ConcurrentHashMap[Pointstamp, Int]
    
    def registerEdge(id: VertexId, ref: Vertex[_]) = {
      edges.put(id, ref)
    }

    def registerVertexIn(graph: Dataflow, source: VertexId, target: VertexId) = {
      val v = graph.getOrElse(source, Nil)
      graph.put(source, target :: v)
    }

    def registerVertex(source: VertexId, target: VertexId) = {
      registerVertexIn(graph, source, target)
      registerVertexIn(reverseGraph, target, source)
    }

    def registerNotify(source: VertexId, target: Vertex[Notify]) = {
      notifyVertex.put(source, target)
    }

    def notifyAt(vertex: VertexId, at: Time) = {
      val targets = notifications.getOrElse(at, Set.empty[VertexId])
      // xxx(okachaiev): concurrency (!!!), should be message most probably
      notifications.put(at, targets + vertex)
    }

    def send[T](source: VertexId, message: Message[T]) = {
      graph.getOrElse(source, Nil) foreach { targetId =>
        edges.get(targetId) match {
          // type level cheating... :(
          case Some(target) => {
            incrementOccurence(Pointstamp(message.time, targetId))
            target.asInstanceOf[Vertex[T]].send((source, message))
          }
          case None => {
            println("[ERROR] Something went very wrong...", source, targetId)
          }
        }
      }
    }

    def sendBy[T](source: VertexId, targetId: VertexId, message: Message[T]) = {
      edges.get(targetId) match {
        case Some(target) => {
          incrementOccurence(Pointstamp(message.time, targetId))
          target.asInstanceOf[Vertex[T]].send((source, message))
        }
        case None => {
          println("[ERROR] Something went very wrong...", source, targetId)
        }
      }
    }

    def notify(point: Pointstamp) = point match { case Pointstamp(at, vertex) =>
      val targets = notifications.getOrElse(at, Set.empty[VertexId])
      notifications.put(at, targets-vertex)
      notifyVertex.get(vertex) map { target =>
        target.send((vertex, Message(at, at)))
      }
    }

    def reachableFromDataflow(graph: Dataflow, vertex: VertexId): Set[VertexId] = {
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
      reachableFromDataflow(reverseGraph, vertex) - vertex

    // xxx(okachaiev): extremely inefficient way to do a traversal
    def reachableFrom(vertex: VertexId): Set[VertexId] =
      reachableFromDataflow(graph, vertex)

    // called when # of message for a specific Vertex at specific Time
    // is dropped to 0. in case if all "previous" nodes in a graph
    // have current round > notification time, we should fire
    // notification
    def tryNotify(point: Pointstamp) = point match { case Pointstamp(at, vertex) =>
      val notifiable = reachableTo(vertex) forall { prev =>
        currentRound.get(prev) match {
          case Some(round) => round > at && occurence.get((prev, at), 0) == 0
          case None => true // xxx(okachaiev): should not happen, right?
        }
      }
      if (notifiable) notify(point)
    }
   
    // xxx(okachaiev): concurrency (!!!), reimplement as messages to
    // rely on a single queue of all updates
    def broadcastProgressUpdate(vertex: VertexId, at: Time) = {
      val round = currentRound.get(vertex)

      // assuming "reachableFrom" also includes itself
      val reachable = reachableFrom(vertex)
      reachable foreach { edge =>
        currentRound.put(edge, Time.max(at, currentRound.get(edge).getOrElse(Time.Epoch)))
      }

      round.map({ at =>
        reachable foreach { edge => tryNotify(Pointstamp(at, edge)) }
      })
    }

    def incrementOccurence(point: Pointstamp) = {
      occurence.compute(point, (_, counter) => {
        if (counter.equals(null)) 1
        else counter + 1
      })
    }

    def shouldNotify(point: Pointstamp): Boolean = point match {
      case Pointstamp(at, vertex) => notifications.get(at) match {
        case Some(edges) => edges.contains(vertex)
        case None => false
      }
    }
    
    def decrementOccurence(point: Pointstamp) = {
      occurence.computeIfPresent(point, (_, counter) => {
        val newCounter = counter-1
        if (newCounter == 0 && shouldNotify(point)) {
          tryNotify(point)
        }
        newCounter
      })
    }

    def newInput[T](): Input[T] = new Input[T](this)
    
    def newOutput(source: VertexId): Edge = {
      val edge = Edge(source, index.getAndIncrement())
      edge
    }

    def subscribe[T](source: Edge, callback: T => Unit): Unit =
      new Subscription(this, source, callback)
  }

  class Input[T](df: Computation) {
    val refId = df.index.incrementAndGet()
    val edge = Edge(-1, refId)
    val isCompleted = new AtomicBoolean(false)
    // relying on the fact that input cannot be a part of
    // any loops :thinking:
    val currentEpoch = new AtomicInteger(0)

    val ref = new SimpleActor[(VertexId, Message[T])]() {
      override def run(message: (VertexId, Message[T])) = {
        df.send(refId, message._2)
        val cursor = currentEpoch.incrementAndGet()
        df.broadcastProgressUpdate(refId, Time(cursor))
      }
    }

    df.registerEdge(refId, ref)

    def onNext(payload: T) = {
      if (!isCompleted.get) ref.send((0, Message(payload, Time(currentEpoch.get))))
    }

    def onComplete() = isCompleted.compareAndSet(false, true)
  }

  // it would be much better to have "sendBy" and "notifyAt"
  // as functions of computation/controller not functions of the vertex
  // itself to avoid coupling between vertex implementation and
  // very specific computation/controller they are intended to be used for
  abstract class UnaryVertex[T, B](df: Computation, source: Edge) {

    val refId = df.index.incrementAndGet()

    val buffers: Map[Time, B] = Map.empty[Time, B]

    def onRecv(edge: Edge, msg: T, time: Time)

    def onNotify(at: Time) = {}
    
    val ref = new SimpleActor[(VertexId, Message[T])]() {
      override def run(message: (VertexId, Message[T])) = message match {
        case (vertexId, message) => {
          onRecv(Edge(vertexId, refId), message.payload, message.time)
          df.decrementOccurence(Pointstamp(message.time, refId))
        }
      }
    }

    val notifyRef = new SimpleActor[(VertexId, Message[Notify])]() {
      override def run(message: (VertexId, Message[Notify])) = {
        onNotify(message._2.time)
      }
    }

    df.registerEdge(refId, ref)

    df.registerVertex(source.target, refId)

    df.registerNotify(refId, notifyRef)

    def sendBy[T](e: Edge, msg: T, time: Time) = df.send(e.target, Message(msg, time))
    def notifyAt(at: Time) = df.notifyAt(refId, at)
  }

  class Subscription[T](df: Computation, source: Edge, callback: T => Unit)
    extends UnaryVertex[T, Any](df, source) {
    def onRecv(e: Edge, msg: T, time: Time) = callback(msg)
  }


}
