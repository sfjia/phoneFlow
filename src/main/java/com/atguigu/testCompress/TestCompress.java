package com.atguigu.testCompress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;

/**
 * @Author 贾
 * @Date 2020/8/3011:33
 */
public class TestCompress {

    public static void main(String[] args) throws Exception {
        //测试压缩
        compress("D:///testcompress.pdf","org.apache.hadoop.io.compress.DefaultCodec");
        compress("D:///testcompress.pdf","org.apache.hadoop.io.compress.BZip2Codec");
        //compress("D:///testcompress.pdf","org.apache.hadoop.io.compress.DefaultCodec.DefaultCodec");
        
        unCompress("D:///testUncompress.gzip");
    }

    /**
     * 解压
     * @param fileName
     */
    private static void unCompress(String fileName) throws Exception {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(fileName);
        //2.获取编码器
        Configuration configuration = new Configuration();
         CompressionCodecFactory codecFactory = new CompressionCodecFactory(configuration);
        CompressionCodec compressionCodec = codecFactory.getCodec(new Path(fileName));
        if(compressionCodec==null){
            System.out.println("compressionCodec = " + compressionCodec);
            return;
        }
        //获取压缩流
        CompressionInputStream cis = compressionCodec.createInputStream(fis);
        //4获取输出流
        int i = fileName.lastIndexOf(".");
        FileOutputStream fos = new FileOutputStream(fileName.substring(0,i));
        IOUtils.copyBytes(cis,fos,configuration);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(cis);
    }


    /**
     * 压缩
     * @param fileName 文件名
     * @param compressCode 压缩方式
     *
     */
    private static void compress(String fileName, String compressCode) throws Exception {
        //1.获取输入流
        FileInputStream fis = new FileInputStream(fileName);
        //获取编码解码器
        Class clazz = Class.forName(compressCode);
        Configuration configuration = new Configuration();
        CompressionCodec compress = (CompressionCodec)ReflectionUtils.newInstance(clazz, configuration);
        //3.获取输出流，（普通文件输入流读取到内存，通过压缩输出流写出到磁盘） ,压缩后缀
        FileOutputStream fos = new FileOutputStream(fileName+compress.getDefaultExtension());
        //4.获取压缩输出流
        CompressionOutputStream cos = compress.createOutputStream(fos);
        //5.流的copy
        IOUtils.copyBytes(fis,cos,configuration);
        //6.关流
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
    }
}
