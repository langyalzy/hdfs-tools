package com.hdfs.compress;

import org.apache.hadoop.conf.*;
import java.util.concurrent.*;
import org.apache.hadoop.util.*;
import java.net.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.slf4j.*;

/**
 * 
 * @Title：CompressFile.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:18:36
 * @Version:1.0
 */
public class CompressFile
{
    private static Logger logger;
    private static String defaultFS;
    private static Configuration configuration;
    private static ExecutorService es;
    private static boolean isAll;
    private Set<String> filePathSet;
    
    public CompressFile() {
        this.filePathSet = new LinkedHashSet<String>();
    }
    
    public static void main(final String[] args) throws Exception {
        if (args.length == 0 || args.length > 5) {
            CompressFile.logger.error("parameters error!!!");
            CompressFile.logger.error("Parameter format:{} {} {} {} {}", "<fsUri>", "<path>", "<type>", "<threads>", "<isAll>");
            CompressFile.logger.error("compress file example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx/ compress true");
            CompressFile.logger.error("uncompress file example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx/ uncompress true");
            return;
        }
        if (args.length == 5) {
            CompressFile.isAll = Boolean.parseBoolean(args[4]);
        }
        final int threadNum = Integer.parseInt(args[3]);
        CompressFile.es = Executors.newFixedThreadPool(threadNum);
        final CompressFile compressFile = new CompressFile();
        final String[] uargs = new GenericOptionsParser(CompressFile.configuration, args).getRemainingArgs();
        CompressFile.defaultFS = uargs[0];
        String sourceDir = uargs[1];
        if (!sourceDir.endsWith("*")) {
            sourceDir = sourceDir.concat("/*");
        }
        final String type = uargs[2];
        if ("compress".equals(type)) {
            compressFile.compress(sourceDir);
        }
        else if ("uncompress".equals(type)) {
            compressFile.uncompress(sourceDir);
        }
        else {
            CompressFile.logger.error("params error");
        }
        while (!CompressFile.es.isTerminated()) {
            try {
                Thread.sleep(5000L);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if ("compress".equals(type)) {
            CompressFile.logger.info("compress job complete, exit...");
        }
        else {
            CompressFile.logger.info("uncompress job complete, exit...");
        }
        CompressFile.logger.info("exit success, bye bye!");
    }
    
    private void compress(final String sourceDir) throws IOException {
        final FileSystem fileSystem = FileSystem.get(URI.create(CompressFile.defaultFS), CompressFile.configuration);
        this.getNeedCompressFilePathList(fileSystem, new Path(sourceDir));
        CompressFile.logger.info("total file num:{}", (Object)this.filePathSet.size());
        for (final String path : this.filePathSet) {
            CompressFile.es.execute(new GzipCompressThread(CompressFile.defaultFS, path, CompressFile.configuration, fileSystem));
        }
        CompressFile.es.shutdown();
        this.filePathSet.clear();
    }
    
    private void uncompress(final String sourceDir) throws IOException {
        final FileSystem fileSystem = FileSystem.get(URI.create(CompressFile.defaultFS), CompressFile.configuration);
        this.getNeedUnCompressFilePathList(fileSystem, new Path(sourceDir));
        CompressFile.logger.info("total file num:{}", (Object)this.filePathSet.size());
        for (final String path : this.filePathSet) {
            CompressFile.es.execute(new GzipUncompressThread(CompressFile.defaultFS, path, CompressFile.configuration, fileSystem));
        }
        CompressFile.es.shutdown();
        this.filePathSet.clear();
    }
    
    private void getNeedCompressFilePathList(final FileSystem fileSystem, final Path path) throws IOException {
        for (final FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile() && !fileStatus.getPath().getName().contains(".gz")) {
                this.filePathSet.add(fileStatus.getPath().toUri().toString());
                CompressFile.logger.info("need compress file num:{}", (Object)this.filePathSet.size());
            }
            if (fileStatus.isDirectory() && CompressFile.isAll) {
                this.getNeedCompressFilePathList(fileSystem, new Path(fileStatus.getPath().toUri().getPath().concat("/*")));
            }
        }
    }
    
    private void getNeedUnCompressFilePathList(final FileSystem fileSystem, final Path path) throws IOException {
        for (final FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile() && fileStatus.getPath().getName().contains(".gz")) {
                this.filePathSet.add(fileStatus.getPath().toUri().toString());
            }
            if (fileStatus.isDirectory() && CompressFile.isAll) {
                this.getNeedUnCompressFilePathList(fileSystem, new Path(path.toUri().getPath().concat("/*")));
            }
        }
    }
    
    static {
        CompressFile.logger = LoggerFactory.getLogger(CompressFile.class);
        CompressFile.defaultFS = null;
        (CompressFile.configuration = new Configuration()).set("dfs.support.append", "true");
        CompressFile.configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        CompressFile.configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        CompressFile.configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        CompressFile.isAll = false;
    }
}
