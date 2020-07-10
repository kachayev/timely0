package timely0

object Collatz extends App {

  import Dataflow._

  final case class Step(root: Long, cursor: Long, steps: Long)

  val computation = new Computation

  val input = computation.newInput[Long]

  val input2step = new UnaryVertex[Long](computation, input.output) {
    def onRecv(e: Edge, msg: Long, time: Time) = {
      this.sendBy(this.output, Step(msg, msg, 0), time)
    }
  }

  val loop = new LoopContext[Step, (Long, Long)](computation, input2step.output) {
    def onRecv(e: Edge, msg: Step, time: Time) = msg match { case Step(root, cursor, steps) =>
      if (cursor == 1) this.sendBy(this.egress, (root, steps), time)
      else {
        val newCursor = if (cursor % 2 == 0) cursor/2 else cursor*3+1
        this.sendBy(this.feedback, Step(root, newCursor, steps+1), time)
      }
    }
  }
 
  computation.subscribe[(Long, Long)](loop.egress, msg => println(msg))
  
  input.onNext(12)
  input.onNext(100)
  input.onNext(1032)
  input.onComplete()
}
