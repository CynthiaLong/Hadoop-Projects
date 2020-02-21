package com.imooc.bigdata.hadoop_train_v2.mr.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AccessMapper extends Mapper<LongWritable,Text, Text, Access>{

	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("\t");

        String phone = lines[1]; // get phone number
        long up = Long.parseLong(lines[lines.length-3]); //get the click in mobile data
        long down = Long.parseLong(lines[lines.length-2]); //get the display mobile data

        context.write(new Text(phone), new Access(phone, up, down));
    }
}
