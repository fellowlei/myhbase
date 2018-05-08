package com.mark.hadoop;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

/**
 * Created by fellowlei on 2018/5/8.
 * https://blog.csdn.net/liuzebin9/article/details/70171338
 */
public class HDFSDemo {
    public static void mkdir(String path) throws IOException {
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);

        Path srcPath =  new Path(path);
        //调用mkdir（）创建目录，（可以一次性创建，以及不存在的父目录）
        boolean flag = fs.mkdirs(srcPath);
        if(flag) {
            System.out.println("create dir ok!");
        }else {
            System.out.println("create dir failure");
        }

        //关闭文件系统
        fs.close();
    }

    /*** 删除文件或者文件目录
     * @throws IOException **/
    public static void rmdir(String filePath) throws IOException {
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);
        Path path = new Path(filePath);

        //调用deleteOnExit(）
        boolean flag = fs.deleteOnExit(path);
        //  fs.delete(path);
        if(flag) {
            System.out.println("delete ok!");
        }else {
            System.out.println("delete failure");
        }

        //关闭文件系统
        fs.close();
    }

    /**创建文件**/
    public static void createFile(String dst , byte[] contents) throws IOException{
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);
        //目标路径
        Path dstPath = new Path(dst);
        //打开一个输出流
        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write(contents);

        //关闭文件系统
        outputStream.close();
        fs.close();
        System.out.println("文件创建成功！");

    }

    /**列出文件**/
    public static void listFile(String path) throws IOException{
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);
        //获取文件或目录状态
        FileStatus[] fileStatus = fs.listStatus(new Path(path));
        //打印文件的路径
        for (FileStatus file : fileStatus) {
            System.out.println(file.getPath());
        }

        //关闭文件系统
        fs.close();
    }

    /**上传本地文件**/
    public static void uploadFile(String src,String dst) throws IOException{
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);
        Path srcPath = new Path(src); //原路径
        Path dstPath = new Path(dst); //目标路径
        //调用文件系统的文件复制函数,前面参数是指是否删除原文件，true为删除，默认为false
        fs.copyFromLocalFile(false,srcPath, dstPath);

        //打印文件路径
        System.out.println("Upload to "+conf.get("fs.default.name"));
        System.out.println("------------list files------------"+"\n");
        FileStatus [] fileStatus = fs.listStatus(dstPath);
        for (FileStatus file : fileStatus) {
            System.out.println(file.getPath());
        }

        //关闭文件系统
        fs.close();
    }

    /**文件重命名**/
    public static void renameFile(String oldName,String newName) throws IOException{
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);
        Path oldPath = new Path(oldName);
        Path newPath = new Path(newName);

        boolean flag = fs.rename(oldPath, newPath);
        if(flag) {
            System.out.println("rename ok!");
        }else {
            System.out.println("rename failure");
        }

        //关闭文件系统
        fs.close();
    }


    public static void readFile(String uri) throws IOException {
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);

        InputStream in = null;
        try {
            in = fs.open(new Path(uri));
            //复制到标准输出流
            IOUtils.copyBytes(in, System.out, 4096,false);
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(in);
        }
    }

    //判断目录是否存在
    public static boolean existDir(String filePath,boolean create) {
        boolean flag = false;
        //判断是否存在
        if(StringUtils.isEmpty(filePath)) {
            return flag;
        }

        Path path = new Path(filePath);
        //读取配置文件
        Configuration conf = new Configuration();
        try {
            //获取文件系统
            FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);

            //或者create为true
            if(create) {
                //如果文件不存在
                if(!fs.exists(path)) {
                    fs.mkdirs(path);
                }
            }

            //判断是否为目录
            if(fs.isDirectory(path)) {
                flag = true;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return flag;
    }

    /**添加到文件的末尾(src为本地地址，dst为hdfs文件地址)
     * @throws IOException */
    public static void appendFile(String src,String dst) throws IOException {
        //读取配置文件
        Configuration conf = new Configuration();
        //获取文件系统
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop1:9000"),conf);
        Path dstPath = new Path(dst);
        //创建需要写入的文件流
        InputStream in = new BufferedInputStream(new FileInputStream(src));

        //文件输出流写入
        FSDataOutputStream out = fs.append(dstPath);
        IOUtils.copyBytes(in, out, 4096,true);

        fs.close();
    }

    public static void main(String[] args) throws IOException {
        //读取文件内容
        //readFile(args[0]);
        //创建文件目录
        /*String s= "hello";
        byte[] bytes = s.getBytes();
        createFile("/liu/h.txt",bytes);*/

        //删除文件
        /*rmdir("/liu2");*/

        //上传文件
        /*uploadFile("/home/liu/hello.text", "/liu/hello.text");*/

        //列出文件
        /*listFile("/liu");*/

        //文科重命名
        /*renameFile("/liu/hi.txt", "/liu/he1.text");*/

        //查询目录是否存在
        /*boolean existDir = existDir("/liu2", false);
        System.out.println(existDir);*/

        //写入文件末尾
        appendFile("/home/liu/hello.text","/liu1/hello.text");
    }



}
