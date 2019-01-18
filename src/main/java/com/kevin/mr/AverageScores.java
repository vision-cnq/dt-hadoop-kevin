package com.kevin.mr;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author kevin
 * @version 1.0
 * @description 平均成绩
 * @createDate 2018/12/18
 */
public class AverageScores {

    public static class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static Text name = new Text();
        private static IntWritable score = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = StringUtils.split(line, " ");
            String strName = fields[0];//学生姓名
            int strScore = Integer.parseInt(fields[1]);//学生单科成绩

            name.set(strName);
            score.set(strScore);
            context.write(name, score);
        }
    }

    public static class ScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private static IntWritable avg_score = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum_score = 0;//统计总成绩
                int count=0;//统计总的科目数
                for (IntWritable score : values) {
                    count++;
                    sum_score += score.get();
                }

                avg_score.set(sum_score / count);
                context.write(key, avg_score);

        }
    }

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 配置输入输出源
        String[] newArgs = new String[]{
                "hdfs://master:9000/test/input_04/",
                "hdfs://master:9000/test/output/" };

        Job job = Job.getInstance(conf);

        job.setJarByClass(AverageScores.class);

        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        job.setOutputKeyClass(Text.class);
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
