package com.kevin.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
 * @description     数据去重
 * @createDate 2018/12/18
 */
public class DataToHeavy {

    public static class DedupMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private static Text field = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            field = value;
            context.write(field, NullWritable.get());

        }
    }

    public static class DedupReducer extends
            Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                              Context context) throws IOException, InterruptedException {

            context.write(key, NullWritable.get());

        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_02/",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);

        job.setJarByClass(DataToHeavy.class);

        job.setMapperClass(DedupMapper.class);
        job.setReducerClass(DedupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        String date = sdf.format(new Date());

        // 指定job处理的数据路径
        FileInputFormat.setInputPaths(job, new Path(newArgs[0]));
        // 指定job处理数据输出结果的路径
        FileOutputFormat.setOutputPath(job, new Path(newArgs[1]+date));

        job.waitForCompletion(true);
    }


}
