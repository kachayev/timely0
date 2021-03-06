package timely0

import scala.collection.mutable.Map

// implementation of code example from Figure 4, section 2.2 "Vertex computation"
// demonstrates difference between immediate (non-coordinated execution) and
// notification-based coordinated flow
object DistinctCount extends App {

  import Dataflow._

  val computation = new Computation 

  val input = computation.newInput[String]

  // there is no such vertex in the code from the paper,
  // but as far as we're going to follow 4.1 code when providing input...
  // we need to make it at least slighly non-trivial
  // the follow vertex accepts a sentence as a single message for entire epoch
  // and splits it into words, sending each word separately while keeping
  // epoch the same. this way, further discount/counter operations would
  // make sense
  // also, `input.output` naming is somewhat missleading and brutally weird...
  // in theory it's possible to avoid the problem by implementing either "output"
  // trait and having implicit conversion from Vertex to Output that uses defaults
  // when necessary. tho' it would be practical, it has nothing to do with the
  // topic of the paper (rather being scala API purity concern)
  val string2words = new UnaryVertex[String](computation, input.output) {
    def onRecv(e: Edge, msg: String, time: Time) = {
      msg.split(" ").map(_.trim().toLowerCase()).foreach({ word =>
        this.sendBy(this.output, word, time)
      })
    }
  }

  val distinctCount = new UnaryVertex[String](computation, string2words.output) {
    // the Naiad implementation has a notion of per-time buffers
    // to simplify user-facing API (it's a typical operation for pretty
    // much each stateful vertex to keep stash of state per each timestamp
    // in a map)
    // here we follow the layout of the code from the paper
    val counts: Map[Time, Map[String, Int]] = Map.empty[Time, Map[String, Int]]

    // this part is not very clear from the paper
    // 4.3 states "... the inputs of a stage must be connected before
    // its outputs, in order to prevent invalid cycles"
    // here, we just declare the output by creating an "Edge" with the source
    // id pointing to a new Vertex and predefined target id that will
    // later be bound to a specific target vertex. which seems to be aligned
    // with the binding order specified in the paper. keeping the code type safe in
    // this case is somewhat cumbersome as the declaration cares not only
    // the fact that we need output here but also what type of messages
    // this output has to accept
    val output1 = computation.newOutput(this.refId)
    val output2 = computation.newOutput(this.refId)

    // keeping Edge as a parameter for each "onRecv" call might not be
    // very practical... often times Vertex get messages from a single
    // source (and we already now exactly how many sources we have when
    // declaring vertex by calling "newUnaryVertex"). when it needs to
    // combine messages from 2 different sources,
    // better API would be onRecv1/onRecv2. 2 sources generalize all
    // other cases pretty well. use this API to remain similarity with
    // the paper
    def onRecv(e: Edge, msg: String, time: Time) = {
      val timeCounts = counts.get(time) match {
        case Some(ct) => ct
        case None =>
          this.notifyAt(time)
          val newCounts = Map.empty[String, Int]
          counts.put(time, newCounts)
          newCounts
      }

      if (!timeCounts.contains(msg)) {
        this.sendBy(output1, msg, time)
      }

      timeCounts.updateWith(msg) {
        case None => Some(1)
        case Some(c) => Some(c+1)
      }
    }

    override def onNotify(time: Time) = {
      counts.get(time).map({ pairs =>
        pairs.foreach({ case pair => this.sendBy(output2, pair, time) })
        counts -= time
      })
    }
  }

  // 4.1 suggests that we can use "Subscribe" to observe changes/outputs
  // seems like the idea of a subscription is a vertex without outputs
  // (vertex that does not emit messages)
  // calling "Subscribe" on "result" variable in the listing seems also
  // too restrictive... here I'm going to use here slightly different
  // approach where we ask computation to create subscription. it seems
  // like a tiny change but it influence entire architecture. e.g. if
  // stage reference is capable of dealing with "Subscribe" -- it has
  // to have at least some notion of the computation itself
  computation.subscribe[String](distinctCount.output1, msg => println(s"distincts: $msg"))
  computation.subscribe[(String, Int)](distinctCount.output2, msg => println(s"counts: $msg"))

  // code example from 4.1
  // as it stated later, OnNext both supplies data *and* advances epoch
  // note, that "timely-dataflow" implementation provides explicit
  // "advancedTo" functionality to deal with epoch separately from
  // data
  input.onNext("All Naiad programms follow a common pattern")
  input.onNext("first define a dataflow graph, consisting of")
  input.onNext("input stages, computational stages, and output stages")
  input.onNext("and then repeatedly supply the input stages with data")

  // means that previous epoch was the last one.
  // basically, graceful shutdown signal to let the scheduler drain all
  // outstanding messages before finishing. not extremely important in case
  // of a single threaded scheduler
  input.onComplete()

}
