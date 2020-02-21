package com.imooc.bigdata.hadoop_train_v2.mr.access;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AccessLocalApp {
	
	static {
	        try {
	            System.load("E:\\Hadoop\\hadoop\\hadoop-2.6.0-cdh5.15.1\\bin\\hadoop.dll");
	        } catch(UnsatisfiedLinkError e) {
	            System.err.println("Native code library failed to load.\n" + e);
	            System.exit(1);
	        }
	    }
				
			
    // Driver端的代码：八股文
    public static void main(String[] args) throws Exception{

    	System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop\\hadoop-2.6.0-cdh5.15.1");
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration);
        job.setJarByClass(AccessLocalApp.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        // sort the data by the starting number of telephone number
        job.setPartitionerClass(AccessPartitioner.class);
        // set the number of reducer
        job.setNumReduceTasks(3);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("E:\\Hadoop\\access.log"));
        FileOutputFormat.setOutputPath(job, new Path("output-access"));

        job.waitForCompletion(true);
    }

}