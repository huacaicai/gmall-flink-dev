package com.zhang.gmall.app.func;

import com.alibaba.fastjson.JSONObject;
import com.zhang.gmall.beans.OrderWide;
import com.zhang.gmall.utils.DimUtil;
import com.zhang.gmall.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @title: 异步查询函数，通过线程池模拟异步发送客户端
 * @author: zhang
 * @date: 2022/3/10 21:33
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimAsyncJoinFunction<T>{
    private ExecutorService threadPool;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        threadPool = ThreadPoolUtil.getThreadPool();
    }


    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        threadPool.submit(
                new Runnable() {
                    @SneakyThrows
                    @Override
                    public void run() {
                        //异步查询逻辑
                        //TODO 1.获取维度查询key
                        String key = getKey(input);
                        //TODO 2.通过key查询维度数据
                        JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);
                        //TODO 3.将维度属性补充到流中对象中
                        join(input,dimInfo);

                        //返回结果
                        resultFuture.complete(Collections.singletonList(input));
                    }
                }
        );
    }
}
