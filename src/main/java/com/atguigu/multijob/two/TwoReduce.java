package com.atguigu.multijob.two;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Test;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/3121:30
 */
public class TwoReduce extends Reducer<Text,Text,Text,Text> {

    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
           sb.append(value.toString()).append("\t");
        }
        v.set(sb.toString());
        context.write(key,v);
    }
}
