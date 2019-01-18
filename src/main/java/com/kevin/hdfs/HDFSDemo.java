package com.kevin.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

/**
 * @author kevin
 * @version 1.0
 * @description 对HDFS实现新增删除，上传下载功能
 * @createDate 2018/12/18
 */
public class HDFSDemo {

    private FileSystem fs = null;
    public static final String HDFS_PATH = "hdfs://Master:9000"; //hdfs路径

    //设置初始化文件	设置hdfs的路径，权限为root
    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException{
        fs = FileSystem.get(new URI(HDFS_PATH), new Configuration(),"root");
    }

    //删除文件	在hdfs删除一个文件（或者文件夹）
    @Test
    public void testDel() throws IllegalArgumentException, IOException{
        boolean flag = fs.delete(new Path("/iamkevin"), true);
        System.out.println(flag);
    }

    //创建文件	在hdfs创建一个文件（或者文件夹）
    @Test
    public void testMkdir() throws IllegalArgumentException, IOException{
        boolean flag = fs.mkdirs(new Path("/iamkevin"));
        System.out.println(flag);
    }

    //上传文件	将本地文件上传到hdfs
    @Test
    public void testUpload() throws IllegalArgumentException, IOException{
        //String fileName = "C:/Users/kevin/Desktop/README";
        String fileName = "D:\\大数据资料\\Hadoop\\hadoop常见的算法\\7.二次排序\\file.txt";
        Path in = new Path(fileName);
        Path out = new Path(HDFS_PATH+"/test/input_06");
        fs.copyFromLocalFile(false, in, out);
        fs.close();
        System.out.println("文件上传成功...");

    }

    //下载文件	将hdfs上的文件下载到本地
    @Test
    public void downloadData() throws IOException {
        InputStream in = fs.open(new Path("/test/word.txt")); //打开hdfs源文件
        FileOutputStream out = new FileOutputStream(new File("C:/Users/kevin/Desktop/hello.txt"));
        IOUtils.copyBytes(in, out, 1024, true); //输出到本地
        fs.close();
        System.out.println("下载成功");
    }

}