package com.xly.gmall.realtime.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * 封装流量域首页、详情页页面浏览数据向下游传递的实体类对象
 */
@Data
@AllArgsConstructor
public class TrafficHomeDetailPageViewBean {
    String stt;
    // 窗口结束时间
    String edt;
    // 首页独立访客数
    Long homeUvCt;
    // 商品详情页独立访客数
    Long goodDetailUvCt;
    // 时间戳
    Long ts;

}
