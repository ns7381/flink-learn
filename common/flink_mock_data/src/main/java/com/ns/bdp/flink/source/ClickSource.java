package com.ns.bdp.flink.source;

import com.ns.bdp.flink.pojo.Click;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class ClickSource implements SourceFunction<Click> {
    @Override
    public void run(SourceContext<Click> ctx) throws Exception {
        long baseTimestamp = System.currentTimeMillis();
        System.out.println("初始时间戳：" + new Timestamp(baseTimestamp));
        List<Click> clickList = new ArrayList<>();
        clickList.add(new Click("user1", baseTimestamp));
        clickList.add(new Click("user1", baseTimestamp + 1000));
        clickList.add(new Click("user1", baseTimestamp + 3000));
        clickList.add(new Click("user1", baseTimestamp + 5000));
        clickList.add(new Click("user1", baseTimestamp + 8000));
        clickList.add(new Click("user2", baseTimestamp + 1000));


        ctx.collect(clickList.get(0));
        Thread.sleep(1000);
        ctx.collect(clickList.get(1));
        ctx.collect(clickList.get(5));
        Thread.sleep(4000);
        ctx.collect(clickList.get(3));
        // baseTimestamp + 3000的这个数据迟到了，但是设置了允许最多迟到4秒，可以继续被处理，并触发两个窗口的merge
        Thread.sleep(1000);
        ctx.collect(clickList.get(2));
        Thread.sleep(2000);
        ctx.collect(clickList.get(4));

    }

    @Override
    public void cancel() {

    }
}
