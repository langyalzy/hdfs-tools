package com.hdfs.compress;

/**
 * 
 * @Title：FileTypeEnum.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:18:27
 * @Version:1.0
 */
public enum FileTypeEnum
{
    SEQUENCE("sequence", 1), 
    TEXT("text", 2), 
    ORC("orc", 3), 
    UNKNOW("unknow", 0);
    
    private String typeName;
    private int index;
    
    private FileTypeEnum(final String typeName, final int index) {
        this.typeName = typeName;
        this.index = index;
    }
    
    public String getTypeName() {
        return this.typeName;
    }
    
    public void setTypeName(final String typeName) {
        this.typeName = typeName;
    }
    
    public int getIndex() {
        return this.index;
    }
    
    public void setIndex(final int index) {
        this.index = index;
    }
}
