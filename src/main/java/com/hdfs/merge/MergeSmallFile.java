package com.hdfs.merge;

import org.apache.hadoop.conf.*;
import java.util.concurrent.*;
import org.apache.hadoop.util.*;
import java.net.*;
import java.util.*;
import org.slf4j.*;
import org.apache.hadoop.io.*;
import java.io.*;
import org.apache.hadoop.fs.*;

/**
 * 
 * @Title：MergeSmallFile.java
 * @Description:用一句话来描述这个类的作用
 * @Author: liaoziyang
 * @Date: 2020年9月14日下午2:19:40
 * @Version:1.0
 */
public class MergeSmallFile
{
    private static Logger logger;
    private static String defaultFS;
    private static String folderPath;
    private static boolean gz;
    private static Configuration configuration;
    private ExecutorService es;
    private static long blockSize;
    private Set<String> needMergerFolderPath;
    
    public MergeSmallFile() {
        this.es = Executors.newFixedThreadPool(2);
        this.needMergerFolderPath = new HashSet<String>();
    }
    
    public static void main(final String[] args) throws IOException {
        if (args.length == 0 || args.length > 3) {
            MergeSmallFile.logger.error("parameters error!!!");
            MergeSmallFile.logger.error("Parameter format:{} {} {}", "<fsUri>", "<path>", "<isGz>");
            MergeSmallFile.logger.error("merge file parameters example: hdfs://xxx.xxx.xxx.xxx:8020 /xxx/xxx/xxx xx");
            return;
        }
        final MergeSmallFile mergeSmallFile = new MergeSmallFile();
        final String[] uargs = new GenericOptionsParser(MergeSmallFile.configuration, args).getRemainingArgs();
        MergeSmallFile.defaultFS = uargs[0];
        MergeSmallFile.folderPath = uargs[1];
        if (uargs.length == 3) {
            MergeSmallFile.gz = Boolean.parseBoolean(uargs[2]);
        }
        MergeSmallFile.logger.info("fsUri:{},path:{}", MergeSmallFile.defaultFS, MergeSmallFile.folderPath);
        mergeSmallFile.merge();
    }
    
    public void merge() throws IOException {
        final FileSystem fileSystem = FileSystem.get(URI.create(MergeSmallFile.defaultFS), MergeSmallFile.configuration);
        this.getNeedMergeFolderPathList(fileSystem, new Path(MergeSmallFile.folderPath));
        MergeSmallFile.logger.info("need merge file num:{}", (Object)this.needMergerFolderPath.size());
        if (this.needMergerFolderPath.size() == 0) {
            return;
        }
        for (final String path : this.needMergerFolderPath) {
            this.es.execute(new MergeThread(path, fileSystem));
        }
        this.es.shutdown();
        while (!this.es.isTerminated()) {
            try {
                Thread.sleep(5000L);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        MergeSmallFile.logger.info("merge job complete, exit...");
        MergeSmallFile.logger.info("exit success, bye bye!");
    }
    
    private void getNeedMergeFolderPathList(final FileSystem fileSystem, final Path path) throws IOException {
        for (final FileStatus fileStatus : fileSystem.globStatus(path)) {
            if (fileStatus.isFile()) {
                this.needMergerFolderPath.add(fileStatus.getPath().getParent().toUri().toString());
                if (this.needMergerFolderPath.size() % 100 == 0) {
                    MergeSmallFile.logger.info("need merge folder num:{}", (Object)this.needMergerFolderPath.size());
                }
            }
            if (fileStatus.isDirectory()) {
                this.getNeedMergeFolderPathList(fileSystem, new Path(fileStatus.getPath().toUri().getPath().concat("/*")));
            }
        }
    }
    
    static {
        MergeSmallFile.logger = LoggerFactory.getLogger(MergeSmallFile.class);
        MergeSmallFile.defaultFS = null;
        MergeSmallFile.folderPath = null;
        MergeSmallFile.gz = false;
        (MergeSmallFile.configuration = new Configuration()).set("dfs.support.append", "true");
        MergeSmallFile.configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        MergeSmallFile.configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        MergeSmallFile.configuration.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        MergeSmallFile.blockSize = 134217728L;
    }
    
    class MergeThread implements Runnable
    {
        String folderPath;
        FileSystem fileSystem;
        Set<FileStatus> oldFiles;
        Set<String> newFiles;
        
        public MergeThread(final String folderPath, final FileSystem fileSystem) {
            this.oldFiles = new HashSet<FileStatus>();
            this.newFiles = new HashSet<String>();
            this.folderPath = folderPath;
            this.fileSystem = fileSystem;
        }
        
        @Override
        public void run() {
            FileStatus[] fileStatuses = new FileStatus[0];
            try {
                fileStatuses = this.fileSystem.globStatus(new Path(this.folderPath + "/*"));
            }
            catch (IOException e) {
                e.printStackTrace();
                return;
            }
            final int fileCount = fileStatuses.length;
            if (fileCount <= 1) {
                return;
            }
            for (int index = 0; index <= fileCount - 1; ++index) {
                if (fileStatuses[index].getLen() < MergeSmallFile.blockSize / 2L) {
                    if (fileStatuses[index].isFile()) {
                        if (!fileStatuses[index].getPath().getName().endsWith(".tmp")) {
                            if (!MergeSmallFile.gz || fileStatuses[index].getPath().getName().contains(".gz")) {
                                if (MergeSmallFile.gz || !fileStatuses[index].getPath().getName().contains(".gz")) {
                                    this.oldFiles.add(fileStatuses[index]);
                                }
                            }
                        }
                    }
                }
            }
            if (this.oldFiles.size() <= 1) {
                return;
            }
            MergeSmallFile.logger.info("{},need merge file count:{}", this.folderPath, this.oldFiles.size());
            long filesSize = 0L;
            Path outFilePath = null;
            FSDataOutputStream out = null;
            try {
                int index2 = 0;
                for (final FileStatus oldFile : this.oldFiles) {
                    ++index2;
                    if (outFilePath == null) {
                        String outFileName = oldFile.getPath().getName().split("\\.")[0] + "." + System.currentTimeMillis();
                        if (oldFile.getPath().getName().contains(".gz") && MergeSmallFile.gz) {
                            outFileName += ".gz";
                        }
                        outFilePath = new Path(this.folderPath + "/" + outFileName);
                        out = this.fileSystem.create(outFilePath);
                        this.newFiles.add(this.folderPath + "/" + outFileName);
                    }
                    final FSDataInputStream in = this.fileSystem.open(oldFile.getPath());
                    IOUtils.copyBytes(in, out, MergeSmallFile.configuration, false);
                    IOUtils.closeStream(in);
                    filesSize += oldFile.getLen();
                    if (filesSize >= MergeSmallFile.blockSize) {
                        filesSize = 0L;
                        outFilePath = null;
                    }
                    if (index2 % 10 == 0 || index2 == fileCount - 1) {
                        MergeSmallFile.logger.info("foler:{},total:{},current:{},size:{}KB,{}MB", this.folderPath, fileCount, index2 + 1, filesSize / 1024L, filesSize / 1048576L);
                    }
                }
                IOUtils.closeStream(out);
                int delCount = 0;
                for (final FileStatus oldFile2 : this.oldFiles) {
                    ++delCount;
                    try {
                        this.fileSystem.delete(oldFile2.getPath(), true);
                    }
                    catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    if (delCount % 10 == 0 || delCount == this.oldFiles.size()) {
                        MergeSmallFile.logger.info("delete old file,filePath:{}, current:{}, total:{}", this.folderPath, delCount, this.oldFiles.size());
                    }
                }
            }
            catch (Exception e2) {
                e2.printStackTrace();
                MergeSmallFile.logger.info("merge fail start rollback...");
                for (final String newFile : this.newFiles) {
                    try {
                        this.fileSystem.delete(new Path(newFile), true);
                    }
                    catch (IOException ex2) {
                        ex2.printStackTrace();
                    }
                }
                MergeSmallFile.logger.info("rollback success...");
            }
            finally {
                IOUtils.closeStream(out);
            }
        }
    }
}
