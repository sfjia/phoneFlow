package com.atguigu.mapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author 贾
 * @Date 2020/8/2913:36
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text,Text , NullWritable> {

    Map<String/*Pid*/,String/* name */> productCache = new HashMap<String, String>();

    /**
     * 先缓存 pd 表
     *
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取输入字节流
        FileInputStream fis = new FileInputStream("D:/input/pd.txt");
        //获取转换流
        InputStreamReader inputStreamReader = new InputStreamReader(fis, "UTF-8");
        //读取数据
        BufferedReader br = new BufferedReader(inputStreamReader);
        String line =null;
        while (StringUtils.isNotBlank(line = br.readLine())){
            String[] fields = line.split("\t");
            //放入缓存
            productCache.put(fields[0],fields[1]);
        }
        //关流
        fis.close();
        inputStreamReader.close();
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行
        String line = value.toString();
        String pid = line.split("\t")[1];
        String pNname = productCache.get(pid);
        String V = line + "\t" + pNname;
        context.write(new Text(V),NullWritable.get());
    }
}
