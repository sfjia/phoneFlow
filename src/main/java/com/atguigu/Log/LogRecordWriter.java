package com.atguigu.Log;

import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @Author 贾
 * @Date 2020/8/2719:15
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream atguigu=null;
    FSDataOutputStream other =null;
    FileSystem fileSystem = null;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            Configuration configuration = job.getConfiguration();
            fileSystem = FileSystem.get(configuration);

            final Path atguiguPath = new Path("f:/atguigu.txt");
             atguigu = fileSystem.create(atguiguPath);

            final Path otherPath = new Path("f:/other.txt");
             other = fileSystem.create(otherPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String line = key.toString();
        if(line.contains("atguigu")){
            atguigu.write(line.getBytes());
        }else {
            other.write(line.getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {

        IOUtils.closeStream(other);
        IOUtils.closeStream(atguigu);
        IOUtils.closeStream(fileSystem);
    }
}
