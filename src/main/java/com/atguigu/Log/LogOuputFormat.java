package com.atguigu.Log;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2719:10
 */
public class LogOuputFormat extends FileOutputFormat<Text, NullWritable> {



    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
        RecordWriter<Text,NullWritable> recordWriter = new LogRecordWriter(job);
        return recordWriter;
    }
}
