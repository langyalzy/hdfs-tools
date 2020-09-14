package com.hdfs.utils;

import java.text.*;
import java.util.*;

/**
 * 
 * @Title：DateUtil.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:19:48
 * @Version:1.0
 */
public class DateUtil
{
    public static final String yyyy_MM_dd_HH_mm_ss = "yyyy-MM-dd HH:mm:ss";
    
    public static String dateToString(final Date date, final String format) {
        final SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            return sdf.format(date);
        }
        catch (Exception e) {
            return null;
        }
    }
    
    public static Date stringToDate(final String date, final String format) {
        final SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            if (sdf.parse(date).getTime() < 0L) {
                return null;
            }
            return sdf.parse(date);
        }
        catch (Exception e) {
            return null;
        }
    }
    
    public static Date GetUTCDate() {
        final Calendar cal = Calendar.getInstance();
        final int zoneOffset = cal.get(15);
        final int dstOffset = cal.get(16);
        cal.add(14, -(zoneOffset + dstOffset));
        return cal.getTime();
    }
}
