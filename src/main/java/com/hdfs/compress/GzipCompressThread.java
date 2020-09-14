package com.hdfs.compress;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * @Title：GzipCompressThread.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:18:01
 * @Version:1.0
 */
@SuppressWarnings("unused")
public class GzipCompressThread implements Runnable
{
    private Logger logger;
    
	private static final String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
    private String defaultFS;
    private String sourceFile;
    private Configuration configuration;
    private FileSystem fileSystem;
    
    public GzipCompressThread(final String defaultFS, final String sourceFile, final Configuration configuration, final FileSystem fileSystem) {
        this.logger = LoggerFactory.getLogger(GzipCompressThread.class);
        this.sourceFile = sourceFile;
        this.defaultFS = defaultFS;
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }
    
    @Override
    public void run() {
        final String gzipFileDir = this.sourceFile.concat(".gz");
        this.logger.info("start compress file:{},target file:{}", this.sourceFile, gzipFileDir);
        Class<?> codecClass = null;
        FSDataInputStream in = null;
        CompressionOutputStream out = null;
        try {
            codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
            final CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, this.configuration);
            final FSDataOutputStream outputStream = this.fileSystem.create(new Path(gzipFileDir));
            in = this.fileSystem.open(new Path(this.sourceFile));
            out = codec.createOutputStream(outputStream);
            IOUtils.copyBytes(in, out, this.configuration);
            this.fileSystem.delete(new Path(this.sourceFile), true);
            this.logger.info("compress success delete:{}", this.sourceFile);
        }
        catch (Exception e) {
            e.printStackTrace();
            this.logger.error("compress file fail,roll back....");
            try {
                if (this.fileSystem != null) {
                    this.fileSystem.delete(new Path(gzipFileDir), true);
                }
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }
    }
}
