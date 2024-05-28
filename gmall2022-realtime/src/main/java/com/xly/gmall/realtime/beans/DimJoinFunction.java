package com.xly.gmall.realtime.beans;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    void getJoin(T object, JSONObject dimInfo);
    String getKey(T object);
}
