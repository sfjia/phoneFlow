package com.atguigu.shareFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/9/139:03
 */
public class SharedFriendTwoReduce extends Reducer<Text,Text,Text,Text> {

    /**
     * g共同好友
     * @param key
     * @param values
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text value : values) {
            sb.append(","+value);
        }
        context.write(key,new Text(sb.substring(1)));
    }
}
