package com.bcdata.analysis.hostpv;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class HostPVReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int pv = 0;
        for (IntWritable value : values) {
            pv += value.get ();
        }

        context.write (key, new IntWritable (pv));
    }
}
