package com.zhang.gmall.app.func;

import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;

/**
 * @title:
 * @author: zhang
 * @date: 2022/3/10 21:51
 */
public interface DimAsyncJoinFunction<T> {
    String getKey(T input);

    void join(T input, JSONObject dimInfo) throws ParseException;
}
