package com.ns.bdp.flink.udf;

import com.ns.bdp.flink.pojo.Commodity;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * 实现TopN的逻辑(小根堆)。
 */
public class TopNFunction extends TableAggregateFunction<Tuple3<Long, String, Integer>, Tuple2<PriorityQueue<Commodity>, List<Commodity>>> {
    private int topSize;

    public TopNFunction(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public Tuple2<PriorityQueue<Commodity>, List<Commodity>> createAccumulator() {
        return Tuple2.of(new PriorityQueue<>(topSize, Comparator.comparingInt(o -> o.sales)), new ArrayList<>(topSize));
    }

    public void accumulate(Tuple2<PriorityQueue<Commodity>, List<Commodity>> acc, Long atime, String pid, Integer sales) {
        int size = acc.f0.size();
        if (size < topSize) {
            acc.f0.add(new Commodity(atime, pid, null, sales));
        } else {
            Commodity min = acc.f0.peek();
            if (min.sales < sales) {
                acc.f0.poll();
                acc.f0.add(Commodity.of(atime, pid, null, sales));
            }
        }
    }


    public void emitValue(Tuple2<PriorityQueue<Commodity>, PriorityQueue<Commodity>> acc,
                          Collector<Tuple3<Long, String, Integer>> out) {
        // TopN记录输出无序，请在下游自行排序。
        for (Commodity ele : acc.f0) {
            out.collect(Tuple3.of(ele.atime, ele.pid, ele.sales));
        }
    }

    public void emitUpdateWithRetract(Tuple2<PriorityQueue<Commodity>, List<Commodity>> acc,
                                      RetractableCollector<Tuple3<Long, String, Integer>> out) {
        // retract回上次保存的TopN但又不属于这次TopN的数据信息。l_ele -> lastElement
        for (Commodity lastElement : acc.f1) {
            if (!acc.f0.contains(lastElement)) {
                out.retract(Tuple3.of(lastElement.atime, lastElement.pid, lastElement.sales));
            }
        }

        // collect这次TopN但又不属于之前TopN的数据信息。
        for (Commodity element : acc.f0) {
            if (acc.f1.isEmpty() || !acc.f1.contains(element)) {
                // TopN记录输出无序，请在下游自行排序。
                out.collect(Tuple3.of(element.atime, element.pid, element.sales));
            }
        }

        // 清空上次保存TopN信息，并保存当前的TopN信息。
        acc.f1.clear();
        for (Commodity ele : acc.f0) {
            acc.f1.add(ele);
        }
    }
}
