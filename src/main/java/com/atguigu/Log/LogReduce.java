package com.atguigu.Log;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2719:08
 */
public class LogReduce extends Reducer<Text,NullPointerException,Text, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<NullPointerException> values, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        String str = line + "\n";
        context.write(new Text(str),NullWritable.get());
    }
}
