package com.ns.bdp.flink.stream.multiStream;

import com.ns.bdp.flink.pojo.Payment;
import com.ns.bdp.flink.pojo.SubmitedOrder;
import com.ns.bdp.flink.pojo.WholeOrder;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        List<SubmitedOrder> submittedOrderList = new ArrayList<>();
        submittedOrderList.add(new SubmitedOrder("order1", "iphone x", 1584964637721L));
        submittedOrderList.add(new SubmitedOrder("order2", "xiaomi 10", 1584964705740L));
        submittedOrderList.add(new SubmitedOrder("order3", "huawei mate30", 1584964984740L));
        KeyedStream<SubmitedOrder, Tuple> submittedOrderStream = env
                .fromCollection(submittedOrderList).keyBy("orderId");

        List<Payment> paymentList = new ArrayList<>();
        paymentList.add(new Payment("order1", "wechat", 1584964687721L));
        paymentList.add(new Payment("order3", "card", 1584964986740L));
        KeyedStream<Payment, Tuple> paymentStream = env
                .fromCollection(paymentList).keyBy("orderId");

        submittedOrderStream
                .connect(paymentStream)
                .flatMap(new Assembly())
                .print();
        env.execute();
    }

    public static class Assembly extends RichCoFlatMapFunction<SubmitedOrder, Payment, WholeOrder> {
        private ValueState<SubmitedOrder> submittedOrderState;
        private ValueState<Payment> paymentState;

        @Override
        public void open(Configuration parameters) throws Exception {
            submittedOrderState = getRuntimeContext().getState(new ValueStateDescriptor<SubmitedOrder>("submitedOrderState", SubmitedOrder.class));
            paymentState = getRuntimeContext().getState(new ValueStateDescriptor<Payment>("paymentState", Payment.class));
        }

        @Override
        public void flatMap1(SubmitedOrder submitedOrder, Collector<WholeOrder> out) throws Exception {
            Payment payment = paymentState.value();
            if (payment != null) {
                out.collect(new WholeOrder(submitedOrder, payment));
            } else {
                submittedOrderState.update(submitedOrder);
            }
        }

        @Override
        public void flatMap2(Payment payment, Collector<WholeOrder> out) throws Exception {
            SubmitedOrder submitedOrder = submittedOrderState.value();
            if (submitedOrder != null) {
                out.collect(new WholeOrder(submitedOrder, payment));
            } else {
                paymentState.update(payment);
            }
        }
    }
}
