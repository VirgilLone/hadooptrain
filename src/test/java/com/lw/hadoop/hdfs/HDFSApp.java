package com.lw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;

/**
 * hdfs java api操作
 * Created by WYluo on 2019/1/7.
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://118.25.129.139:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/HDFSApi"));
    }

    @Test
    public void createFile() throws Exception {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/HDFSApi/aa.txt"));
        fsDataOutputStream.write("hello hadoooop1".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    @Test
    public void cat()throws Exception{
        FSDataInputStream open = fileSystem.open(new Path("/HDFSApi/aa.txt"));
        IOUtils.copyBytes(open,System.out,1024);
        open.close();
    }

    @Test
    public void rename() throws Exception{
        Path res= new Path("/HDFSApi/aa.txt");
        Path tar= new Path("/HDFSApi/bb.txt");
        fileSystem.rename(res,tar);
    }

    /**
     * 本地复制文件到hdfs（上传）
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("D:\\copyTest.txt");
        Path hdfsPath = new Path("/HDFSApi");
        fileSystem.copyFromLocalFile(localPath,hdfsPath);
    }

    /**
     * 带进度上传
     * @throws Exception
     */
    @Test
    public void copyFromLocalFileWithProgressbar() throws Exception{
        InputStream inputStream=new BufferedInputStream(new FileInputStream(new File("本地的文件目录")));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/HDFSApi/copyBigFile.tgz"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");// 带进度提醒
            }
        });

        IOUtils.copyBytes(inputStream,fsDataOutputStream,4096);
    }

    /**
     * 下载
     * @throws Exception
     */
    @Test
    public void copyToLocal() throws Exception{
        Path localPath = new Path("D:\\");
        Path hdfsPath = new Path("/test/a/b/h.txt");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    @Test
    public void delete() throws Exception{
        fileSystem.delete(new Path("/HDFSApi/aa.txt"),true);
    }

    /**
     * 列出文件系统某个目录的所有文件
     * @throws Exception
     */
    @Test
    public void listHdfsFile() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/HDFSApi"));
        for(FileStatus fileStatus : fileStatuses){
            String isDir=fileStatus.isDirectory()?"文件夹":"文件";
            long len = fileStatus.getLen();
            String path = fileStatus.getPath().toString();
            short replication = fileStatus.getReplication();

            System.out.println(isDir+"\t"+replication+"\t"+path+"\t"+len);
        }

    }



    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration,"luowen");//这是hdfs系统的用户名
        System.out.println("BEFORE end");
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("hdfs.tearDown...");
    }
}
