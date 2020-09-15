package com.atguigu.multijob.two;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/3121:25
 */
public class TwoMapper extends Mapper<LongWritable, Text,Text,Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split("--");
        String[] split = words[1].split("\t");
        k.set(words[0]);
        v.set(split[0]+"-->"+split[1]);
        context.write(k,v);
    }
}
