package com.xly.gmall.realtime.beans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 标记不需要向ck表中传递的属性
 */
@Target(ElementType.FIELD)//作用范围是属性。还可设置方法等
@Retention(RetentionPolicy.RUNTIME)//什么时候执行完将被舍弃
public @interface TransientSink {
}
