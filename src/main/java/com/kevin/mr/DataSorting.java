package com.kevin.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author kevin
 * @version 1.0
 * @description 数据排序
 * @createDate 2018/12/18
 */
public class DataSorting {

    //map将输入中的 value化成 IntWritable类型，作为输出的 key
    public static class SortMapper extends
            Mapper<LongWritable, Text, IntWritable, IntWritable> {

        private static IntWritable data = new IntWritable();
        private static final IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            data.set(Integer.parseInt(line));
            context.write(data, one);
        }
    }

    /**
     *     reduce 将输入中的 key 复制到输出数据的 key 上，
     *     然后根据输入的 value‐list 中元素的个数决定 key 的输出次数用
     *     全局linenumber来代表key的位次
     */
    public static class SortReducer extends
            Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private static IntWritable linenumber = new IntWritable(1);

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(linenumber, key);
                linenumber.set(linenumber.get() + 1);
                // linenumber=new IntWritable(linenumber.get()+1);
            }

        }
    }

    public static void main(String[] args) throws IllegalArgumentException,
            IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_03/",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);

        job.setJarByClass(DataSorting.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        // 指定job处理的数据路径
        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        // 指定job处理数据输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));

        job.waitForCompletion(true);
    }




}
