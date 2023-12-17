package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.mapper.ContaHashtagsMapper;
import org.example.reduce.ContaHashtagsReducer;

import java.io.IOException;

public class Main {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "conta hashtags");

        job.setJarByClass(Main.class);

        job.setMapperClass(ContaHashtagsMapper.class);

        job.setReducerClass(ContaHashtagsReducer.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("bases"));

        FileOutputFormat.setOutputPath(job, new Path("saida"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}