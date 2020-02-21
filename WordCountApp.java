package com.imooc.bigdata.hadoop_train_v2.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

public class WordCountApp {
	
    static {
        try {
            System.load("E:\\Hadoop\\hadoop\\hadoop-2.6.0-cdh5.15.1\\bin\\hadoop.dll");
        } catch(UnsatisfiedLinkError e) {
            System.err.println("Native code library failed to load.\n" + e);
            System.exit(1);
        }
    }
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop\\hadoop-2.6.0-cdh5.15.1");
		// System.setProperty("HADOOP_USER_NAME", "hadoop");
		
		Configuration conf = new Configuration();
		// conf.set("fs.defaultFS", "hdfs://192.168.1.213:8020");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(WordCountApp.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//both of map and reduce function return void
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("input/wc.input"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		
		boolean result = job.waitForCompletion(true);
		
		System.exit(result ?0 : -1);
	}

}
