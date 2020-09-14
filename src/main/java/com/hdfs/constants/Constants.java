package com.hdfs.constants;

import java.nio.charset.*;

/**
 * 
 * @Title：Constants.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:19:29
 * @Version:1.0
 */
public interface Constants
{
    public static final Charset GLOBAL_CHARSET = Charset.forName("utf-8");
    public static final String CORE_XML_PATH = "/config/*.xml";
}
