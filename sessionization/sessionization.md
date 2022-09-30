# Improved session aggregation for the Spark Dataset runner in Apache Beam. {ignore=true}

[Apache Beam](https://beam.apache.org/about/) is an open-source SDK for data pipelines that unifies batch and stream processing in a single API and supports various languages and runtime environments <!--- execution environments?? -->, such as Apache Spark, Apache Flink or Dataflow.

Apache Beam plays an important part in many of our product offerings at Talend and we are committed to contribute to the ongoing evolution of Beam.

[TOC]

## Runners in Beam

The responsibility of a runner in Beam is to translate Beam pipelines into a native pipeline for the respective runtime environment, e.g. Apache Spark, and run the pipeline. This abstraction obviously provides great flexibility to users.
To learn more about Beam runners and how the translation of pipelines is done, I recommend reading [this introduction](https://echauchot.blogspot.com/2020/02/understand-apache-beam-runners-focus-on.html) of our colleague Etienne.

Beam's runner for Apache Spark is based on Spark's `RDD` API (in batch mode). However, there's also a new, experimental runner using Spark SQL with the more recent `Dataset` API. In the following we're going to explore how to improve the performance of aggregations on [Session windows](https://beam.apache.org/documentation/programming-guide/#session-windows) for that runner.

A `Dataset` in Spark depends on an `Encoder` for (de-)serialization of data similar to `Coder`s in Beam.
These convert a data point from and to a binary representation and are critical to efficiently move data between nodes of the distributed compute system.
In the following we are going to ignore that aspect as it's a subject of its own.

## Aggregations in Beam

Aggregations in Beam aka [`Combine` transforms](https://beam.apache.org/documentation/programming-guide/#combine) are based on associative and commutative `CombineFn` functions.

In the following we're looking to translate a `Combine.PerKey<KeyT, InputT, OutputT>`. Simply said, this is an aggregation per key.

```java
public abstract class CombineFn<InputT, AccumT, OutputT>{

  // A new empty accumulator.
  public abstract AccumT createAccumulator();

  // Add a value to the accumulator.
  public abstract AccumT addInput(AccumT accumulator, InputT input);

  // Merge multiple accumulators.
  public abstract AccumT mergeAccumulators(Iterable<AccumT> accumulators);

  // Extract the output of the resulting accumulator.
  public abstract OutputT extractOutput(AccumT accumulator);

  ...
}
```

The respective counterpart in Spark is an `Aggregator`, which is functionally very similar to the above:

```java
public abstract class Aggregator<InputT, AccumT, OutputT> {

  // A new empty accumulator.
  public abstract AccumT zero()

  // Add a value to the accumulator.
  public abstract AccumT reduce(AccumT accumulator, InputT input)

  // Merge two accumulators.
  public abstract AccumT merge(AccumT accumulator1, AccumT accumulator2)

  // Extract the output of the resulting accumulator.
  public abstract OutputT finish(AccumT accumulator)

  ...
}
```

### Aggregations on non-merging windows

Windows sub-devide datasets according to timestamps of individual data points, e.g. the data per fixed hourly window.
The concept of windowing is critical when working with unbounded streams of data, but it also applies to batch processing, and scopes
transformations such as aggregations to the respective window.

In Beam's [windowing model](https://beam.apache.org/documentation/programming-guide/#windowing) every data point is assigned to one or several windows and
represented as `WindowedValue<T>`. In batch this is by default a single global window, meaning everything.
Our colleague Alexey wrote an [introduction to Beam windowing](https://aromanenko.blogspot.com/2019/03/data-processing-using-beam-part-3.html) if you're looking for further information.

Fixed, non-merging bounded windows, including the single global window, are the simplest case when translating `Combine.PerKey<KeyT, InputT, OutputT>` transforms.
These windows are independent of windows of other data points, so we don't have to evaluate and potentially merge them in the Spark aggregator.
Instead we can move the windows into a composite key (BoundedWindow, KeyT).
Especially if there's only few keys, this can even help to better scale out and mitigate potential problems due to skew in the data.

However, a windowed value `WindowedValue<T>` could be assigned to multiple windows. For instance, that's the case with sliding windows:

Consider a sliding window of 1 hour that advances ever 5 minutes. For each key we get 12 new composite keys.
That raises the question if we risk to massively increase the amount of data that needs to be shuffled around when assigning each value to 12 instead of just 1 key when grouping the data by the composite key?

Luckily no, Spark will perform a map-side (aka local) `reduce` using the `Aggregator` first.
This means all rows of a partition belonging to the same key will be reduced into an accumulator locally.
Afterwards, these intermediate accumulators, containing a partial aggregation each, will be shuffled and merged.
This takes advantage of the associative and commutative property of the aggregation (aka combine) function.

The following code snippet shows the translation of `Combine.PerKey` transforms on non-merging windows in the experimental Spark runner:

```java

final CombineFn<InputT, AccumT, OutputT> combineFn;
final Dataset<WindowedValue<KV<KeyT, InputT>>> dataset;

...

final Aggregator<InputT, AccumT, OutputT> inputAgg = new Aggregator<InputT, AccumT, OutputT> {

  @Override
  public AccumT zero() {
    return combineFn.createAccumulator();
  }

  @Override
  public AccumT reduce(AccumT accum, InputT in) {
    return combineFn.addInput(accum, in);
  }

  @Override
  public AccumT merge(AccumT accum1, AccumT accum2) {
    return combineFn.mergeAccumulators(ImmutableList.of(accum1, accum2));
  }

  @Override
  public OutputT finish(AccumT accum) {
    return combineFn.extractOutput(accum);
  }

  ...
};

Dataset<WindowedValue<KV<KeyT, OutputT>>> aggregatedDataset = dataset

  // explode combinations of BoundedWindow and KV pair (KeyT, InputT) to tuples of composite key (BoundedWindow, KeyT) and value InputT
  .flatMap(explodeWindowedKey(), ...) // Dataset<Tuple2<Tuple2<BoundedWindow, KeyT>, InputT>>

  // group dataset by composite key
  .groupByKey(Tuple2::_1, ...) // KeyValueGroupedDataset<Tuple2<BoundedWindow, KeyT>, Tuple2<Tuple2<BoundedWindow, KeyT>, InputT>>

  // drop composite key on the value side (the aggregation input)
  .mapValues(Tuple2::_2, ...) // KeyValueGroupedDataset<Tuple2<BoundedWindow, KeyT>, InputT>

  // aggregate InputT per composite key to OutputT
  .agg(inputAgg.toColumn()) // Dataset<Tuple2<Tuple2<BoundedWindow, KeyT>, OutputT>>

  // wrap aggregated KV pair (KeyT, OutputT) into WindowedValue of respective BoundedWindow
  .map(windowedKV(), ...);

```

### Aggregations on session windows

Sessions are bounded time intervals of activity separated by periods of no input, the gap duration.
Other than the fixed, non-merging windows discussed above, sessions windows are dynamic in nature and depend on windows of other data points.

The following graph taken from [Beam documentation](https://beam.apache.org/documentation/programming-guide/#session-windows) illustrates this.

![Session windows](https://beam.apache.org/images/session-windows.png)

In Beam, by convention, we append the gap duration to each window.
This time, the aggregator has to keep track of windows and merge these, as well as associated accumulators, if they intersect.

`Aggregator` and `CombineFn` cannot share the same accumulator type `AccumT` anymore, as done for non-merging windows above.
To not confuse the different accumulators, we are going to instead call the aggregation state of the `Aggregator` buffer from now on.

#### Canonical window merging in Beam

Beam's **canonical implementation** to merge session windows is very general, but not well fitted for this use case. Have a look at the following code snippet from [<img src="github.png" style="height:16px;"/> apache/beam](https://github.com/apache/beam/blob/5520fe064fc3b7196998d4597746119691eb6681/sdks/java/core/src/main/java/org/apache/beam/sdk/transforms/windowing/MergeOverlappingIntervalWindows.java#L39-L63), in particular the highlighted lines:

```java {.line-numbers, highlight=6-10}
public static void mergeWindows(WindowFn<?, IntervalWindow>.MergeContext c) throws Exception {
  // Merge any overlapping windows into a single window.
  // Sort the list of existing windows so we only have to
  // traverse the list once rather than considering all
  // O(n^2) window pairs.
  List<IntervalWindow> sortedWindows = new ArrayList<>();
  for (IntervalWindow window : c.windows()) {
    sortedWindows.add(window);
  }
  Collections.sort(sortedWindows);
  List<MergeCandidate> merges = new ArrayList<>();
  MergeCandidate current = new MergeCandidate();
  for (IntervalWindow window : sortedWindows) {
    if (current.intersects(window)) {
      current.add(window);
    } else {
      merges.add(current);
      current = new MergeCandidate(window);
    }
  }
  merges.add(current);
  for (MergeCandidate merge : merges) {
    merge.apply(c);
  }
}
```

For each input, we'd be creating a copy of the current windows (all windows in the buffer plus all additional windows assigned to the input) and sort them to find merge candidates.
Especially when the number of session windows is large, this is becoming more and more a performance issue. Further down this can be well-observed in the benchmarks.

#### Optimized session aggregator

To prevent such performance issues, a session aggregator implementation would require to efficiently
- check the buffer for a preceding (or started simultaneously) window and all subsequent windows that intersect with the input window without having to repeatedly sort windows;
- remove intersecting windows from the buffer;
- and last but not least, add new windows.

Sorted, tree based maps are likely the most obvious fit for above requirements.
In the following we're going to explore an implementation based on Java's `TreeMap`, a self-balancing Red-Black tree.

The aggregator will be of the following type:

```java
Aggregator<
  // Windowed key-value pairs as input
  WindowedValue<KV<KeyT, InputT>>,
  // TreeMap based aggregation buffer to track accumulated inputs per session IntervalWindow
  TreeMap<IntervalWindow, AccumT>,
  // Collection of windowed output values, one per accumulated session
  Collection<WindowedValue<OutputT>>>
>
```

**Note**:

To reduce complexity for demonstration purposes, we assume a `WindowingStrategy` with timestamp combiner `END_OF_WINDOW`.
Support for Beam's windowing model is still partial that way.
Similar to the way combine functions aggregate input values, timestamp combiners aggregate input timestamps to assign one resulting timestamp to the windowed aggregation result `WindowedValue<OutputT>`.

In case of the default combiner `END_OF_WINDOW`, this timestamp is always the end of the session `IntervalWindow` and doesn't depend on individual input timestamps. A feature-complete implementation supporting timestamp combiners can be found on [<img src="github.png" style="height:16px;"/> apache/beam](https://github.com/apache/beam/blob/3a4d57eb8976c5f503b32d478a80b1800490f66f/runners/spark/3/src/main/java/org/apache/beam/runners/spark/structuredstreaming/translation/batch/Aggregators.java).

##### Adding values to the aggregation buffer

Using the sorted tree based aggregation buffer, we can traverse the tree in order starting from the window preceding (or matching the start of) the input window if it intersects the input window, or from the following window otherwise. From there we continue to traverse the tree as long as windows intersect the input window which we continuously expand during the process. Alongside that, we have to merge corresponding accumulators.

Once done, we have to remove all windows we've merged while traversing the tree. And finally add the input value to the merged accumulator (or a new empty accumulator otherwise) and insert it into the tree together with the expanded window.
Adding the input last to the accumulator helps avoiding unnecessary allocations as we only create a new, empty accumulator if needed.

The following sequence of `reduce` operations for a single key illustrates the above process. The input is highlighted in red:

1. No window precedes input `7` and `[4]` doesn't intersect; create an empty accumulator `[]`, add `7` to it and insert it (into the tree).
    ```mermaid
    gantt
        dateFormat  Y
        axisFormat  %y
        todayMarker off

        section Key 1
        7   :crit,1, 3
        [4] :6,9
        [2] :14,15
    ```

1. The previous `[4]` doesn't intersect input `5`, nor does `[2]`; create an empty accumulator `[]`, add `5` to it and insert it.
    ```mermaid
    gantt
        dateFormat  Y
        axisFormat  %y
        todayMarker off

        section Key 1
        [7] :1, 3
        [4] :6,9
        5   :crit,10,12
        [2] :14,15
    ```

1. `[4]` matches start of input `4`, create a window that spans both; the following `[5]` doesn't intersect; remove accumulator `[4]` and its window (from the tree), add `4` to `[4]` and insert the updated window and accumulator.
    ```mermaid
    gantt
        dateFormat  Y
        axisFormat  %y
        todayMarker off

        section Key 1
        [7] :1,3
        4   :crit,6,8
        [4] :6,9
        [5] :10,12
        [2] :14,15
    ```


1. The previous `[7]` doesn't intersect input `4`, but `[8]` does; create a window that spans both; `[5]` doesn't intersect anymore; remove accumulator `[8]` and its window, add `4` to `[8]` and insert the updated window and accumulator.
    ```mermaid
    gantt
        dateFormat  Y
        axisFormat  %y
        todayMarker off

        section Key 1
        [7] :1,3
        4   :crit,5,7
        [8] :6,9
        [5] :10,12
        [2] :14,15
    ```

1. The previous `[7]` intersects input `9`, create a window that spans both; `[12]` intersects as well, expand the window again and merge `[12]` into `[7]`; `[5]` still intersects, expand the window again and merge `[5]` into `[12+7]`; `[2]` doesn't intersect anymore; remove `[12]` and `[5]` and their windows; finally add `9` to `[12+7+5]` and insert the updated window and accumulator.
    ```mermaid
    gantt
      dateFormat  Y
      axisFormat  %y
      todayMarker off

      section Key 1
      [7] :1,3
      9   :crit,3,11
      [12] :5,9
      [5] :10,12
      [2] :14,15
    ```


  1. After the sequence of `reduce` operations, our aggreagation buffer for "Key 1" looks as follows:
      ```mermaid
      gantt
        dateFormat  Y
        axisFormat  %y
        todayMarker off

        section Key 1
        [33] :1,12
        [2] :14,15
      ```


The code below implements the algorithm described above. As mentioned above, you can find the full, feature-complete implementation on [<img src="github.png" style="height:16px;"/> apache/beam](https://github.com/apache/beam/blob/3a4d57eb8976c5f503b32d478a80b1800490f66f/runners/spark/3/src/main/java/org/apache/beam/runners/spark/structuredstreaming/translation/batch/Aggregators.java#L188-L228).

```java
private final Combine.CombineFn<InputT, AccumT, OutputT> fn;

@Override
public TreeMap<IntervalWindow, AccumT> reduce(TreeMap<IntervalWindow, AccumT> buff, WindowedValue<KV<KeyT, InputT>> input) {
  // for each input window
  for (IntervalWindow window : (Collection<IntervalWindow>) input.getWindows()) {
    AccumT acc = null;
    IntervalWindow first = null, last = null;
    // start with window before or equal to new window (if exists)
    Map.Entry<IntervalWindow, AccumT> lower = buff.floorEntry(window);
    if (lower != null && window.intersects(lower.getKey())) {
      // if intersecting, init accumulator and extend window to span both
      acc = lower.getValue();
      window = window.span(lower.getKey());
      first = last = lower.getKey();
    }
    // merge following windows in order if they intersect, then stop
    for (Map.Entry<IntervalWindow, AccumT> entry : buff.tailMap(window, false).entrySet()) {
      AccumT entryAcc = entry.getValue();
      IntervalWindow entryWindow = entry.getKey();
      if (window.intersects(entryWindow)) {
        // extend window and merge accumulators
        window = window.span(entryWindow);
        acc = acc == null ? entryAcc : fn.mergeAccumulators(ImmutableList.of(acc, entryAcc));
        if (first == null) {
          // there was no previous (lower) window intersecting the input window
          first = last = entryWindow;
        } else {
          last = entryWindow;
        }
      } else {
        break; // stop, later windows won't intersect either
      }
    }
    if (first != null && last != null) {
      // remove entire subset from from first to last after it got merged into acc
      buff.navigableKeySet().subSet(first, true, last, true).clear();
    }

    if (acc == null) {
      // init accumulator if necessary
      acc = fn.createAccumulator();
    }
    // add input and set accumulator for new (potentially merged) window
    buff.put(window, fn.addInput(acc, input.getValue().getValue()));
  }
  return buff;
}
```


##### Merging aggregation buffers

The idea to merge our tree buffers is simple; we traverse both trees in order and always pick the next smallest window according to the window start time. While it intersects a previous one, we expan the windows and merge the accumulators, if not we add it to a new tree buffer and continue until there's no more windows; once the final window is added we're done.

The following code implements the merge operation. Once again, the full, feature-complete implementation is available on [<img src="github.png" style="height:16px;"/> apache/beam](https://github.com/apache/beam/blob/3a4d57eb8976c5f503b32d478a80b1800490f66f/runners/spark/3/src/main/java/org/apache/beam/runners/spark/structuredstreaming/translation/batch/Aggregators.java#L231-L271).

```java
public TreeMap<IntervalWindow, AccumT> merge(TreeMap<IntervalWindow, AccumT> buff1, TreeMap<IntervalWindow, AccumT> buff2) {
  if (buff1.isEmpty()) {
    return buff2;
  } else if (buff2.isEmpty()) {
    return buff1;
  }
  // Init new tree map to merge both buffers
  TreeMap<IntervalWindow, AccumT> res = zero();
  PeekingIterator<Map.Entry<IntervalWindow, AccumT>> it1 =
      Iterators.peekingIterator(buff1.entrySet().iterator());
  PeekingIterator<Map.Entry<IntervalWindow, AccumT>> it2 =
      Iterators.peekingIterator(buff2.entrySet().iterator());

  AccumT acc = null;
  IntervalWindow window = null;
  while (it1.hasNext() || it2.hasNext()) {
    // pick iterator with the smallest window ahead and forward it
    Map.Entry<IntervalWindow, AccumT> nextMin =
        (it1.hasNext() && it2.hasNext())
            ? it1.peek().getKey().compareTo(it2.peek().getKey()) <= 0 ? it1.next() : it2.next()
            : it1.hasNext() ? it1.next() : it2.next();
    if (window != null && window.intersects(nextMin.getKey())) {
      // extend window and merge accumulators if windows intersect
      window = window.span(nextMin.getKey());
      acc = fn.mergeAccumulators(ImmutableList.of(acc, nextMin.getValue()));
    } else {
      // store window / accumulator if necessary and continue with next minimum
      if (window != null && acc != null) {
        res.put(window, acc);
      }
      acc = nextMin.getValue();
      window = nextMin.getKey();
    }
  }
  if (window != null && acc != null) {
    res.put(window, acc);
  }
  return res;
}
```

## Benchmarks

### Nexmark

Apache Beam contains a benchmark suite called Nexmark consisting of multiple "real-world" queries over a three entities model representing an online auction system.
If you're interested to learn more, I recommend reading the [introduction to nexmark](https://echauchot.blogspot.com/2020/06/nexmark-benchmark-and-ci-tool-for.html) of our colleague Etienne.

Nexmark doesn't necessarily give an accurate overall number as it's running locally and certain aspects such as serialization and shuffling data are simply ignored.
Nevertheless, it will give us a decent estimate how the optimized aggregator performs compared to an aggregator using the canonical window merging of Beam when reducing partitions.

In the following we're looking at Nexmark Query 11 (user sessions).

##### Results

The boxplot charts below visualize runtime and throughput for 1 million and 10 million input events based on 10 individual benchmark runs in either case.
The black bar depicts the median.

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {"url": "query11.csv"},
  "spacing": 100,
  "hconcat": [{
    "title": "1 million events",
    "hconcat": [
      {
        "width": 100,
        "height": 300,
        "mark": {"type": "boxplot", "median": {"stroke": "black", "strokeWidth": 1}},
        "transform": [{"filter": {"field": "Events", "equal": "1000000"}}],
        "encoding": {
          "x": {
            "field": "Aggregator",
            "type": "nominal",
            "title": null,
            "axis": {"labels": false}
          },
          "color": {"field": "Aggregator", "type": "nominal", "scale": {"scheme": "set2"}},
          "y": {
            "field": "Runtime (secs)",
            "type": "quantitative",
            "scale": {"zero": false},
            "axis": {"tickCount":40, "labelSeparation":1}
          }
        }
      },
      {
        "width": 100,
        "height": 300,
        "mark": {"type": "boxplot", "median": {"stroke": "black", "strokeWidth": 1}},
        "transform": [{"filter": {"field": "Events", "equal": "1000000"}}],
        "encoding": {
          "x": {
            "field": "Aggregator",
            "type": "nominal",
            "title": null,
            "axis": {"labels": false}
          },
          "color": {"field": "Aggregator", "type": "nominal"},
          "y": {
            "field": "Throughput (per sec)",
            "type": "quantitative",
            "scale": {"domain": [480000, 610000]},
            "axis": {"tickCount": 40, "labelSeparation":10}
          }
        }
      }
    ]
  },{
    "title": "10 million events",
    "hconcat": [
      {
        "width": 100,
        "height": 300,
        "mark": {"type": "boxplot", "median": {"stroke": "black", "strokeWidth": 1}},
        "transform": [{"filter": {"field": "Events", "equal": "10000000"}}],
        "encoding": {
          "x": {
            "field": "Aggregator",
            "type": "nominal",
            "title": null,
            "axis": {"labels": false}
          },
          "color": {"field": "Aggregator", "type": "nominal"},
          "y": {
            "field": "Runtime (secs)",
            "type": "quantitative",
            "scale": {"zero": false},
            "axis": {"tickCount": 20, "labelSeparation":10}
          }
        }
      },
      {
        "width": 100,
        "height": 300,
        "mark": {"type": "boxplot", "median": {"stroke": "black", "strokeWidth": 1}},
        "transform": [{"filter": {"field": "Events", "equal": "10000000"}}],
        "encoding": {
          "x": {
            "field": "Aggregator",
            "type": "nominal",
            "title": null,
            "axis": {"labels": false}
          },
          "color": {"field": "Aggregator", "type": "nominal"},
          "y": {
            "field": "Throughput (per sec)",
            "type": "quantitative",
            "scale": {"domain": [660000, 790000]},
            "axis": {"tickCount": 40, "labelSeparation":10}
          }
        }
      }
    ]
  }]
}
```

- For both aggregators we observe a much lower throughput for the small dataset.
  This indicates a fairly large general runtime overhead, as likely expected by everyone familiar with Beam and Spark.
- The runtime of the **optimized aggregator** is better in both cases.
  For the larger dataset the median runtime decreased by 13%.
  For the small dataset the improvement is rather insignificant due to the overhead of the system.


### JMH micro-benchmark

We expect the `reduce` operation to be the bottleneck of the aggregator: every single input row must be reduced into an aggregation buffer. The number of required merges, on the other hand, corresponds to the number of partitions in the system and is fairly low.

In the following we're going to compare the `reduce` throughput (**Reduce Ops/sec**) of the aggregators using Java micro-benchmarks. Events are generated such that new sessions are created with a certain probability (**New session**) (or get merged otherwise). Specifically, we're looking at two scenarios:

1. **Random inserts**: Unless a new session is created (in the future), the event is randomly merged with one or several existing sessions in the past.

2. **Sequential inserts**: Unless a new session is created (in the future), the event is merged into the previous session.

A significant difference between the two scenarios is the amount of session windows getting merged during a reduce operation, which is much higher in scenario 1. Consequently, the overall number of sessions doesn't grow as quickly.

##### Results

The boxplot charts below visualize reduce operations per second (reduce throughput) for both scenarios based on 25 benchmark iterations each.
For better readability the two aggregators are presented separately on different scales.
The black bar depicts the median throughput.

```vega-lite
{
  "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
  "data": {"url": "jmh-result.json"},
  "spacing": 100,
  "transform": [
    {"flatten": ["primaryMetric.rawData"], "as": ["Reduce Ops/sec"]},
    {"flatten": ["Reduce Ops/sec"]},
    {"calculate": "{'true': 'Optimized', 'false': 'Canonical'}[datum.params.optimize]", "as": "Aggregator"},
    {"calculate": "{'true': 'Sequential inserts', 'false': 'Random inserts'}[datum.params.ordered]", "as": "Scenario"}
  ],
  "hconcat": [{
    "width": 100,
    "height": 300,
    "mark": {"type": "boxplot", "median": {"stroke": "black", "strokeWidth": 1}},
    "transform": [{"filter": {"field": "Aggregator", "equal": "Canonical"}}],
    "encoding": {
      "color": {"field": "Aggregator"},
      "x": {"field": "params.newSessionProbability", "type": "nominal", "title": "New session"},
      "y": {"field": "Reduce Ops/sec", "type": "quantitative","scale": {"zero": false},"axis":{"tickCount":20, "labelSeparation":10}},
      "column": {"field": "Scenario", "title": null}
    }
  },{
    "width": 100,
    "height": 300,
    "mark": {"type": "boxplot", "median": {"stroke": "black", "strokeWidth": 1}},
    "transform": [{"filter": {"field": "Aggregator", "equal": "Optimized"}}],
    "encoding": {
      "color": {"field": "Aggregator"},
      "x": {"field": "params.newSessionProbability", "type": "nominal", "title": "New session"},
      "y": {"field": "Reduce Ops/sec", "type": "quantitative", "title":null, "scale": {"zero": false}, "axis":{"tickCount": 20,"labelSeparation":10}},
      "column": {"field": "Scenario", "title": null}
    }
  }]
}
```

- Reduce throughput of the **optimized aggregator** is orders of magnitude higher (400 to 750x).

- The higher the probability of creating new sessions for the **canonical aggregator**, the more reduce throughput drops.
  - This is due to the overhead of sorting session windows as part of Beam's canonical window merging (see above).
  Obviously, the more windows there are, the more expensive this gets.
  - In scenario 1 this effect is alleviated as the growth factor is reduced due to additional merges.
  However, the reduce throughput is also far less in this case. The costs of merging intersecting session windows and replacing them in the buffer decreases throughput to up to 1/3.

- The **optimized aggregator** shows the opposite picture.
  Larger number of sessions are handled efficiently and throughput increases for higher new session probabilities.
  - For scenario 1 we observe a similar behavior as seen for the canonical aggregator. Throughput is also reduced to up to 1/3 due to the costs of merging interseting session windows and manipulating the buffer accordingly.

The full benchmark code is available on [<img src="github.png" style="height:16px;"/> apache/beam](https://github.com/apache/beam/commit/af41fbfde5d559cfd810eced62e0a4486dbb5daa) for reference.

## Summary

Using a specialized aggregator on session windows, we can avoid performance bottlenecks due to limitations in Beam's canonical window merging and boost the overall performance of pipelines containing aggregations over session windows when using the experimental Spark dataset runner.
The actual performance gain will highly depend on the input datasets, but can be very significant if the number of sessions per key is high.

With the effort presented here we are pushing the experimental Spark dataset runner one step further towards an efficient and reliable runner for Apache Beam. Nevertheless, there's a lot more work left to do.
