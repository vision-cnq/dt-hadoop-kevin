package com.kevin.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author kevin
 * @version 1.0
 * @description     简单的WordCount示例，单词计数
 * @createDate 2018/12/17
 */
public class WordCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration(); // 读取hadoop配置文件

        Job job = Job.getInstance(conf, "word count"); // 新建一个Job类，传入配置信息
        job.setJarByClass(WordCount.class); // 设置主类
        job.setMapperClass(TokenizerMapper.class); // 设置map类
        job.setCombinerClass(IntSumReducer.class); // 设置combiner类
        job.setReducerClass(IntSumReducer.class); // 设置reduce类
        job.setOutputKeyClass(Text.class); // 设置输出类型key
        job.setOutputValueClass(IntWritable.class); // 设置输出类型value
        FileInputFormat.addInputPath(job, new Path("hdfs://192.168.171.100:9000/test/word.txt")); // 设置输入文件
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.171.100:9000/test/output_02")); // 设置输出文件
        System.exit(job.waitForCompletion(true) ? 0 : 1); // 等待完成退出
    }
}
