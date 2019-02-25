package com.lw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
