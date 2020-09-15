package com.atguigu.shareFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/9/138:30
 */
public class SharedFriendReduce extends Reducer<Text,Text,Text,Text> {


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text person : values) {
            sb.append(","+person);
        }
        context.write(key,new Text(sb.substring(1)));
    }
}
