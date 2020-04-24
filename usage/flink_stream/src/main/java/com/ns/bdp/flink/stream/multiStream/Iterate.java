package com.ns.bdp.flink.stream.multiStream;

import com.ns.bdp.flink.source.RandomFibonacciSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


public class Iterate {
    private static final int BOUND = 100;
    private static OutputTag<Tuple5<Integer, Integer, Integer, Integer, Integer>> feedback = new OutputTag<Tuple5<Integer, Integer, Integer, Integer, Integer>>("needAnotherIteration") {
    };

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource(BOUND));
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> iteration = inputStream
                .map(new InputMap())
                .iterate();
        SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Integer>> normalOutPut = iteration.process(new Step());
        DataStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> feedbackStream = normalOutPut.getSideOutput(feedback);
        iteration.closeWith(feedbackStream);
        normalOutPut.map(new OutputMap()).print();

        env.execute("Streaming Iteration Example");
    }

    /**
     * 为了后续统计方便，将二元组制作成五元组，第三第四个值初始化为前两个值的副本，第五个值记录迭代次数
     */
    public static class InputMap implements MapFunction<Tuple2<Integer, Integer>,
            Tuple5<Integer, Integer, Integer,
                    Integer, Integer>> {
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }


    /**
     * 斐波那契迭代，a和b被替代为b和a+b，到达设定阈值的输出，不符合的回到上个算子继续迭代
     */
    public static class Step extends ProcessFunction<Tuple5<Integer, Integer,
            Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {

        @Override
        public void processElement(Tuple5<Integer, Integer, Integer, Integer, Integer> value, Context ctx,
                                   Collector<Tuple5<Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
            Tuple5<Integer, Integer, Integer, Integer, Integer> result = new Tuple5<>(value.f0, value.f1, value.f3, value.f2 + value.f3, ++value.f4);
            if (result.f2 < BOUND && result.f3 < BOUND) {
                ctx.output(feedback, result);
            } else {
                out.collect(result);
            }

        }
    }

    /**
     * 给出原始值和迭代次数
     */
    public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
            Tuple2<Tuple2<Integer, Integer>, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer> value) {
            return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
        }
    }

}
