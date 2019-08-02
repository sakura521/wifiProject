package com.uestc.bigdata.common.time;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *  时间转换类，当前时间转换成yyyy-mm-dd形式
 */
public class TimeTransformUtils {
    private static Logger LOG= LoggerFactory.getLogger(TimeTransformUtils.class);

    private static Date nowTime;
    public static String Date2yyyy_MM_dd(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        nowTime = new Date(System.currentTimeMillis());
        String time = dateFormat.format(nowTime);
        return time;
    }


    public static String Long2yyyyMMdd(long timeLong){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        nowTime = new Date(timeLong*1000);
        String time = dateFormat.format(nowTime);
        return time;
    }

    public static String Date2yyyyMMdd(String timeLong){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        nowTime = new Date(Long.valueOf(timeLong)*1000);
        String time = dateFormat.format(nowTime);
        return time;
    }


    public static void main(String[] args) {
        System.out.println(Long2yyyyMMdd(1540884324));
    }
}
