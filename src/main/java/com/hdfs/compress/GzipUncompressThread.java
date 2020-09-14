package com.hdfs.compress;

import org.apache.hadoop.conf.*;
import org.slf4j.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.*;
import java.io.*;
import org.apache.hadoop.io.compress.*;

/**
 * 
 * @Title：GzipUncompressThread.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:19:20
 * @Version:1.0
 */
public class GzipUncompressThread implements Runnable
{
    private Logger logger;
    private static final String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
    private String defaultFS;
    private String sourceFile;
    private Configuration configuration;
    private FileSystem fileSystem;
    
    public GzipUncompressThread(final String defaultFS, final String path, final Configuration configuration, final FileSystem fileSystem) {
        this.logger = LoggerFactory.getLogger(GzipUncompressThread.class);
        this.sourceFile = path;
        this.defaultFS = defaultFS;
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }
    
    @Override
    public void run() {
        if (!this.sourceFile.contains(".gz")) {
            this.logger.error("file:{} is not gz file.", this.sourceFile);
            return;
        }
        final String goal_dir = this.sourceFile.replace(".gz", "");
        this.logger.info("start uncompress file:{},target file:{}", this.sourceFile, goal_dir);
        Class<?> codecClass = null;
        FSDataInputStream input = null;
        OutputStream output = null;
        try {
            codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
            final CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, this.configuration);
            input = this.fileSystem.open(new Path(this.sourceFile));
            final CompressionInputStream codec_input = codec.createInputStream(input);
            output = this.fileSystem.create(new Path(goal_dir));
            IOUtils.copyBytes(codec_input, output, this.configuration);
            this.fileSystem.delete(new Path(this.sourceFile), true);
            this.logger.info("compress success delete:{}", this.sourceFile);
        }
        catch (Exception e) {
            e.printStackTrace();
            this.logger.error("compress file fail,roll back....");
            if (this.fileSystem != null) {
                try {
                    this.fileSystem.delete(new Path(goal_dir), true);
                }
                catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        finally {
            IOUtils.closeStream(input);
            IOUtils.closeStream(output);
        }
    }
}
