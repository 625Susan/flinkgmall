package com.xly.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.xly.gmall.realtime.beans.DimJoinFunction;
import com.xly.gmall.realtime.utils.DimUtil;
import com.xly.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/*dws层异步IO关联外部维度数据封装通用类
继承RichAsyncFunction，实现其中的方法
抽象定义方法，方便子类来实现
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T> {
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //开启线程
        ThreadPoolExecutor poolExecutor = ThreadPoolUtil.getInstance();
        poolExecutor.submit(new Runnable() {
            @Override
            public void run() {
                //获取流中的key值
               String key= getKey(obj);
                //通过key值到表中查询到对象
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, key);//tableName可以通过new对象时，子类自己传递过来
                //将对象补充维度到流中
                //如果能够获取到对象
                if (dimInfo != null){
                    getJoin(obj,dimInfo);  //计算，具体的实现
                }
                //将数据向下游传递
                resultFuture.complete(Collections.singleton(obj));
            }
        });
    }
}
