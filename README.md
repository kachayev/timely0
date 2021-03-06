# Timely Zero

Minimalistic implementation of Naiad paper "A Timely Dataflow System" in Scala.

## What

Quoting Microsoft Research:

> The Naiad project is an investigation of data-parallel dataflow computation, like Dryad and DryadLINQ, but with a focus on low-latency streaming and cyclic computations. Naiad introduces a new computational model, timely dataflow, which combines low-latency asynchronous message flow with lightweight coordination when required. These primitives allow the efficient implementation of many dataflow patterns, from bulk and streaming computation to iterative graph processing and machine learning.

## Why?

Even though the original idea of multidimensional timestamps for progress tracking to allow for cycles in dataflow graph described in the ["Naiad: A Timely Dataflow System"](https://cs.stanford.edu/~matei/courses/2015/6.S897/readings/naiad.pdf) seems pretty straighforward, understanding of details and implementation techniques might be somewhat... tricky.

The goal of the project is to find a simplest possible implementation of all concepts described in the paper. So the reader does not need to understand 22,700 lines of C# code (estimate from the paper). Another problem with learning the concepts... the reference Naiad [implementation](https://github.com/MicrosoftResearch/Naiad) has both a lot of low level details (e.g. handling network communication between distributed nodes) and higher level frameworks (e.g. GraphLINQ). Those are incredibly valueable but at the same time they overcomplicate understanding of the basics.

It does not seem like Naiad paper got a lot of industry traction (apart from the fact that some ideas from the paper were used as architectural foundation for TensorFlow). Newer implementation of Timely Dataflow concept is done in Rust here: [timely-dataflow](https://github.com/TimelyDataflow/timely-dataflow). Rust library has much clear approach to defining dataflow primities (e.g. stages, scopes, etc) and relies on the concept of built-in iterables rather than on inheritance. Which makes it somewhat easier to understand the code but the concepts are still hard to grasp without prior learning.

## How?

The project contains implementation of 2 cases:
* `DistinctCount` from the paper
* `CollatzConjecture` as a simple example of dataflow loops

Message-passing between nodes of the dataflow graph is implemented using Actors (leveraging the simplest actors library ever, [castor](https://github.com/lihaoyi/castor)). Actor-based implementation would definitely suffer from performance problems but the concept of Actors sending messages plays nicely with core idea of dataflow nodes exchanging messages to progress time (Vertex API described in the paper is almost identical to typical Actors systems with `sendBy` and `onRecv`). Also, using message-passing instead of direct state mutation allows us to abstract away the notion that nodes might run on different machines. In this case, more advance libraries like Akka would handle networking keeping the high-level API similar to a single machine execution context.

Note, that progress tracking would work the same way even when running on distributed nodes. The fact was briefly mentioned in the paper and formally proved in ["Formal Analysis of a Distributed Algorithm for Tracking Progress"](https://www.microsoft.com/en-us/research/wp-content/uploads/2013/06/clock-verif2.pdf) (this paper introduces formal TLA specification).

Vertex API seems too verbose in many cases, and it's true. It is verbose. Basic concepts were never meant to be used directly in high-level applications. Instead, timely dataflow provides the platform to build friendlier frameworks on top of it, e.g. [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow) that uses functional transformations of collections of data with pretty familiar operators like `map`, `filter`, `join`, `group` etc or [GraphLINQ](https://bigdataatsvc.wordpress.com/2014/05/08/graphlinq-a-graph-library-for-naiad/) that provides streaming interface over graph definitions with nodes/edges and values attached to them.

Even `subscription` functionality that is used to observe changes hapenning within dataflow graph seems quite high level (and, in fact, is implementated by reusing existing Vertex abstraction).

Also, see a lot of comments in the code around specific decisions made along the road.

## DOs

What is covered:

* multidimensional time (epoch & loop counters)
* dataflow graph (vertecies to process data, edges to form connections)
* loop context (ingress, feedback, egress)
* reachability analysis ("could-result-in" concept from the paper)
* progress tracker, pointstamps, occurence counters

## DONTs

What was intentionally omitted/skipped/over-simplified:

* single process (no networking, single scheduler)
* data partition between workers/nodes
* debug/tracability (e.g. vertex names, dynamic subscription etc)
* graceful shutdown, stage flush
* checkpoint/restore vertex state

Notable performance improvements that could/should be done:
* reachability in a dataflow graph is recomputed on each message instead of recomputing it when new vertex is introduced

## Compile & Run

Just use `sbt`.

```shell
$ sbt
[info] Loading project definition from /Users/okachaiev/timely0/project
[info] Loading settings for project root from build.sbt ...
[info] Set current project to timely0 (in build file:/Users/okachaiev/timely0/)
sbt:timely0> run
[info] running timely0.DistinctCount
distincts: all
distincts: naiad
distincts: programms
distincts: follow
distincts: a
distincts: supply
distincts: the
distincts: with
distincts: data
...
counts: (output,1)
counts: (all,1)
counts: (the,1)
counts: (stages,,2)
counts: (consisting,1)
counts: (a,2)
counts: (with,1)
counts: (input,2)
...
```

## Licence

Copyright © 2020 `timely0`

`timely0` is licensed under the MIT license, available at MIT and also in the LICENSE file.
