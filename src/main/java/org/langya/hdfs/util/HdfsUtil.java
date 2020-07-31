package org.langya.hdfs.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

/**
 * 
 **************************************************************      
 **************************************************************                                                      
 *                    <br><b>深圳易仓科技</b>                                                                        
 *             <br><b>https://www.eccang.com/</b><br>                                                 
 **************************************************************
 **************************************************************  
 * @Title：HdfsUtil.java
 * @Description:
 * hdfs工具类，实现以下功能：<br>
 * 1、得到hdfs文件系统对象<br>
 * 2、获取hdfs目录的所有文件名称或者子目录名称<br>
 * 3、重命名hdfs文件
 * @Author: liaoziyang
 * @Date: 2020年7月30日上午10:45:27
 * @Version:20200730(优化版本)
 */
public class HdfsUtil {
	
	//hdfs集群HA地址，不是单节点
	static final String HDFS_CLUSTER = "hdfs://nameservice-jk";
	//hdfs目录连接符号
	static final String HDFS_DIR_JOIN = "/";
	//hdfs临时文件开头标志
	static final String HDFS_TMP_FILE_FIX = ".";
									
	/**
	 * 
	 * @MethodName: main
	 * @Description: 程序主入口
	 * @author liaoziyang
	 * @param args void
	 * @date 2020-07-31 11:45:29
	 */
	public static void main(String[] args) {

		try {
			getlistFilesRename(args[0]);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * 
	 * @MethodName: getHdfs 
	 * @Description: 得到hdfs操作对象
	 * @author liaoziyang
	 * @return hdfs操作对象
	 * @throws IOException FileSystem
	 * @date 2020-07-31 11:44:51
	 */
	public static FileSystem getHdfs() throws IOException {

		Configuration conf = new Configuration();
		
		return FileSystem.get(URI.create(HDFS_CLUSTER), conf);
	}
	
	/**
	 * 
	 * @MethodName: rename
	 * @Description: 修改hdfs文件名称
	 * @author liaoziyang
	 * @param fromFile 原文件名称（全路径）
	 * @param toFile 修改成为的文件名称（全路径）
	 * @throws IOException void
	 * @date 2020-07-31 11:03:55
	 */
	public static void rename( String fromFile,String toFile) throws IOException{
		FileSystem fs = getHdfs(); 
		fs.rename(new Path(fromFile), new Path(toFile));
		fs.close();
	}
	
	/**
	 * 
	 * @MethodName: getFilesAndDirs
	 * @Description: 得到指定目录下面的文件数组
	 * @author liaoziyang
	 * @param hdfsDir 需要修改临时文件为正式文件的的hdfs文件路径
	 * @return 指定hdfs目录下面的所有文件路径集合
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException Path[]
	 * @date 2020-07-31 11:05:55
	 */
	public static Path[] getFilesAndDirs(String hdfsDir) throws FileNotFoundException, IllegalArgumentException, IOException{
		FileStatus[] fileStatus = getHdfs().listStatus(new Path(hdfsDir));
		return FileUtil.stat2Paths(fileStatus);
	}
	
	/**
	 * 
	 * @MethodName: getlistFilesRename
	 * @Description: 循环遍历指定目录下的所有文件名，并判断是否为临时文件，如果为临时文件，则修改为正式文件
	 * @author liaoziyang
	 * @param hdfsDir 需要修改临时文件为正式文件的的hdfs文件路径
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException void
	 * @date 2020-07-31 11:07:05
	 */
	public static void getlistFilesRename(String hdfsDir) throws FileNotFoundException, IllegalArgumentException, IOException {
		
		/**
		 * 1、循环遍历得到的hdfs文件目录地址
		 * 1.1、判断是否为临时文件
		 * 1.2、如果为临时文件，则调用重命名hdfs文件方法
		 * 1.2.1、替换掉集群hdfs的前缀，只传递路径，不需要带hdfs前缀（HDFS_CLUSTER），替换之后的名称规则：直接截图掉临时文件的第一个"。"
		 */
		for(Path p : getFilesAndDirs(hdfsDir)) {
			if(p.getName().indexOf(HDFS_TMP_FILE_FIX) == 0) {
				rename(p.getParent().toString().replace(HDFS_CLUSTER, "") + HDFS_DIR_JOIN + p.getName(), p.getParent().toString().replace(HDFS_CLUSTER, "") + HDFS_DIR_JOIN + (p.getName().substring(1, p.getName().length())));
			}
		}
	}
	

}
