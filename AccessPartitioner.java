package com.imooc.bigdata.hadoop_train_v2.mr.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * define the groups that will be sent to the same reducer
 */
public class AccessPartitioner extends Partitioner<Text, Access>{

    
    @Override
    public int getPartition(Text phone, Access access, int numReduceTasks) {

        if(phone.toString().startsWith("13")) {
            return 0;
        } else if(phone.toString().startsWith("15")) {
            return 1;
        } else {
            return 2;
        }
    }
}
