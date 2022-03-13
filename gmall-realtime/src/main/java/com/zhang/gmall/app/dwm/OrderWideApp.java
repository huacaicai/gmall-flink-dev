package com.zhang.gmall.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.zhang.gmall.app.func.DimAsyncFunction;
import com.zhang.gmall.beans.OrderDetail;
import com.zhang.gmall.beans.OrderInfo;
import com.zhang.gmall.beans.OrderWide;
import com.zhang.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @title: 订单宽表
 * @author: zhang
 * @date: 2022/3/10 19:41
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点设置

        //TODO 3.读取订单数据、转化为pojo类、分配时间戳和提取水位线
        String orderTopic = "dwd_order_info";
        String groupId = "order_wide_app_dev";
        SingleOutputStreamOperator<OrderInfo> orderInfoKafkaDS = env
                .addSource(KafkaUtil.getKafkaSource(orderTopic, groupId))
                .map(new RichMapFunction<String, OrderInfo>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String value) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                        //yyyy-MM-dd HH:mm:ss
                        String createTime = orderInfo.getCreate_time();
                        String[] dateTimeArr = createTime.split(" ");
                        orderInfo.setCreate_date(dateTimeArr[0]);
                        orderInfo.setCreate_hour(dateTimeArr[1].split(":")[0]);
                        orderInfo.setCreate_ts(sdf.parse(createTime).getTime());
                        return orderInfo;
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        //TODO 4.读取订单明细数据、转化为pojo类，分配时间戳和提取水位线
        String orderDetailTopic = "dwd_order_detail";
        SingleOutputStreamOperator<OrderDetail> orderDetailKafkaDS = env
                .addSource(KafkaUtil.getKafkaSource(orderDetailTopic, groupId))
                .map(new RichMapFunction<String, OrderDetail>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String value) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                        String createTime = orderDetail.getCreate_time();
                        orderDetail.setCreate_ts(sdf.parse(createTime).getTime());
                        return orderDetail;
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                        return element.getCreate_ts();
                                    }
                                })
                );

        //TODO 5.订单和订单明细进行双流join
        SingleOutputStreamOperator<OrderWide> orderWideJoinDS = orderInfoKafkaDS
                .keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailKafkaDS.keyBy(OrderDetail::getOrder_id))
                .between(Time.seconds(-10L), Time.seconds(5L))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });
        //打印测试
        orderWideJoinDS.map(JSON::toJSONString).print("orderWideJoinDS");

        //TODO 6.关联用户维度
        //实现分发请求的 AsyncFunction
        //获取数据库交互的结果并发送给 ResultFuture 的 回调 函数
        //将异步 I/O 操作应用于 DataStream 作为 DataStream 的一次转换操作。
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDimDS = AsyncDataStream
                .unorderedWait(orderWideJoinDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide input, JSONObject dimInfo) throws ParseException {
                        input.setUser_gender(dimInfo.getString("GENDER"));
                        String birthday = dimInfo.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Long currentTime = System.currentTimeMillis();
                        Long time = sdf.parse(birthday).getTime();
                        Long age = (currentTime - time) / 1000 / 60 / 60 / 24 / 365L;
                        input.setUser_age(age.intValue());
                    }
                }, 60, TimeUnit.SECONDS);
        orderWideWithUserDimDS.print("orderWideWithUserDimDS");
        //TODO 7.关联地区维度
        //TODO 8.关联商品维度
        //TODO 9.关联品牌维度
        //TODO 10.关联品类维度
        //TODO 11.关联spu维度
        //TODO 12.写入kafka主题
        //TODO 13.执行任务
        env.execute("OrderWideApp");
    }
}
