package com.hdfs.bean;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.*;
import org.springframework.context.*;
import org.springframework.context.support.*;

/**
 * 
 * @Title：BeanFactory.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:18:49
 * @Version:1.0
 */
public class BeanFactory
{
    public static Logger logger;
    public static Map<String, Object> beansMap;
    private static ApplicationContext Appctx;
    private static ApplicationContext xmlPathApplication;
    private static XmlPathBean xmlPathBean;
    
    public static void loadBeansByApp(final ApplicationContext Appctx) {
        final Map<String, Object> map = Appctx.getBeansOfType(Object.class);
        final String[] beanDefinitionNames;
        final String[] beanNames = beanDefinitionNames = Appctx.getBeanDefinitionNames();
        for (final String key : beanDefinitionNames) {
            BeanFactory.beansMap.put(map.get(key).getClass().getName(), map.get(key));
            BeanFactory.beansMap.put(key, map.get(key));
        }
    }
    
    public static <T> T getBean(final Class objClass) {
        Object obj = null;
        try {
            obj = BeanFactory.beansMap.get(objClass.getName());
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return (T)obj;
    }
    
    public static <T> T getBean(final String id) {
        Object obj = null;
        try {
            obj = BeanFactory.beansMap.get(id);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        return (T)obj;
    }
    
    public static <T> Map<String, T> getBeans(final Class objClass) {
        final Map<String, T> map = new HashMap<String, T>();
        for (final String key : BeanFactory.beansMap.keySet()) {
            if (BeanFactory.beansMap.get(key).getClass().getName().equals(objClass.getName())) {
                map.put(key, (T)BeanFactory.beansMap.get(key));
            }
        }
        return map;
    }
    
    private static String getFilePathPrefix() {
        final String prefix = BeanFactory.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        return prefix;
    }
    
    public static void main(final String[] args) {
        System.out.println(getFilePathPrefix());
    }
    
    static {
        BeanFactory.logger = LoggerFactory.getLogger(BeanFactory.class);
        BeanFactory.beansMap = new HashMap<String, Object>();
        try {
            BeanFactory.xmlPathApplication = new ClassPathXmlApplicationContext("/config/*.xml");
            try {
                BeanFactory.xmlPathBean = BeanFactory.xmlPathApplication.getBean(XmlPathBean.class);
            }
            catch (NoSuchBeanDefinitionException e) {
                BeanFactory.logger.warn("{}", e.getMessage());
            }
            if (BeanFactory.xmlPathBean != null) {
                BeanFactory.Appctx = new ClassPathXmlApplicationContext((String[])BeanFactory.xmlPathBean.xmlPathList.toArray(new String[BeanFactory.xmlPathBean.xmlPathList.size()]));
            }
            else {
                BeanFactory.Appctx = BeanFactory.xmlPathApplication;
            }
            loadBeansByApp(BeanFactory.Appctx);
        }
        catch (Exception e2) {
            e2.printStackTrace();
        }
    }
}
