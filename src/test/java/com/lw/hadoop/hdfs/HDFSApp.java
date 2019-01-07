package com.lw.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        fileSystem.mkdirs(new Path("/HDFSApi/test"));
    }

    @Test
    public void createFile() throws Exception {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/HDFSApi/test/kk.txt"));
        fsDataOutputStream.write("hello hadoooooop".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }




    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration);
    }

    @After
    public void tearDown() {
        configuration = null;
        fileSystem = null;
        System.out.println("hdfs.tearDown...");
    }
}
