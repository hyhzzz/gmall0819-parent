package com.atguigu.gmall.realtime.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author coderhyh
 * @create 2022-04-27 11:54
 * 自定义日期转换的工具类
 * <p>
 * SimpleDateFormat存在线程安全的问题
 * 在封装日期工具类的时候，推荐使用JDK1.8之后提供的相关类型
 *  DateTimeFormatter ---- SimpleDateFormat
 *  LocalDateTime     ---- Date
 *  Instant           ---- Calendar
 */
public class DateTimeUtil {
    //private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    //将日期对象转换为字符串日期
    public static String toYMDHMS(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    //将字符串类型日期数据转换成Long类型时间毫秒数
    public static long toTs(String dateStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(dateStr, dtf);
        Long ts = localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
        return ts;

    }

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }
}
