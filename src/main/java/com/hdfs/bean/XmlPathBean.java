package com.hdfs.bean;

import java.io.*;
import java.text.*;
import java.util.*;

/**
 * 
 * @Title：XmlPathBean.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:18:42
 * @Version:1.0
 */
public class XmlPathBean implements Serializable
{
    String env;
    public List<String> xmlPathList;
    
    public XmlPathBean() {
        this.xmlPathList = new ArrayList<String>();
    }
    
    public List<String> getXmlPathList() {
        return this.xmlPathList;
    }
    
    public void setXmlPathList(final List<String> xmlPathList) {
        for (final String xmlPath : xmlPathList) {
            this.xmlPathList.add(MessageFormat.format(xmlPath, this.env));
        }
    }
    
    public String getEnv() {
        return this.env;
    }
    
    public void setEnv(final String env) {
        this.env = env;
    }
}
