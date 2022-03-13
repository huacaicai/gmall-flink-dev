package com.zhang.gmall.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zhang.gmall.utils.KafkaUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @title: 日志分流
 * @author: zhang
 * @date: 2022/3/11 18:28
 */
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 2.设置检查点
      /*  env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/gamll-dev"));
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L));
        System.setProperty("HADOOP_USER_NAME", "zhang");*/


        //TODO 3.读取kafka ods层数据，转换结构
        String topic = "ods_base_log_2022";
        String groupId = "base_log_app_dev";
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = env
                .addSource(KafkaUtil.getKafkaSource(topic, groupId))
                .map(JSON::parseObject);


        //TODO 4.新老用户修正
        SingleOutputStreamOperator<JSONObject> repairDS = kafkaJsonDS
                .keyBy(data -> data.getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    //定义保存用户上次登录时间状态
                    private ValueState<String> timeState;
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        timeState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>(
                                        "time-state", Types.STRING
                                )
                        );

                        sdf = new SimpleDateFormat("yyyy-MM-dd");
                    }

                    @Override
                    public void processElement(JSONObject element, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取用户is_new值来进行判断是否需要修复
                        String isNew = element.getJSONObject("common").getString("is_new");
                        //只有当不是新用户的时候才进行校验修复 1
                        if ("1".equals(isNew)) {
                            //获取状态上次登录状态
                            String lastTime = timeState.value();
                            Long ts = element.getLong("ts");
                            String curryTime = sdf.format(new Date(ts));
                            //TODO 小功能：如果业务需求只需对一天时间内无论登录多少次都是新访客，在if判断加上判断时期是否今天
                            //lastTime != null && lastTime.length() > 0 && !curryTime.equals(lastTime)
                            if (lastTime != null && lastTime.length() > 0 && !curryTime.equals(lastTime)) {
                                //修复
                                element.getJSONObject("common").put("is_new", 0);
                            } else {
                                timeState.update(curryTime);
                            }
                        }
                        out.collect(element);
                    }
                });

        //TODO 5.数据分流，页面日志--主流、曝光日志--侧输出流、启动日志--侧输出流
        //定义侧输出标签
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        SingleOutputStreamOperator<String> resultDS = repairDS
                .process(new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject element, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        JSONObject startObj = element.getJSONObject("start");
                        if (startObj != null && startObj.size() > 0) {
                            //启动日志输出到侧输出流
                            ctx.output(startTag, element.toJSONString());
                        } else {
                            //不是启动日志即是页面日志，页面日志包括曝光日志
                            out.collect(element.toJSONString());
                            JSONArray jsonArray = element.getJSONArray("displays");
                            if (jsonArray != null && jsonArray.size() > 0) {
                                for (int i = 0; i < jsonArray.size(); i++) {
                                    JSONObject displayObj = jsonArray.getJSONObject(i);
                                    //为所有曝光信息添加页面id和ts字段
                                    displayObj.put("ts", element.getLong("ts"));
                                    displayObj.put("page_id", element.getJSONObject("page").getString("page_id"));
                                    ctx.output(displayTag, displayObj.toJSONString());
                                }
                            }
                        }
                    }
                });

        //TODO 6.提取侧输出流数据
        resultDS.getSideOutput(startTag).print("start");
        resultDS.getSideOutput(displayTag).print("display");
        resultDS.print("page");
        //TODO 7.发送到kafka dwd层对应主题
        //TODO 8.启动任务
        env.execute();
    }
}
