package com.bcdata.analysis.raduis_user;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RadiusUserReducer extends Reducer<CompositeKey, IntWritable, CompositeKey, IntWritable> {
    @Override
    protected void reduce (CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int pv = 0;
        for (IntWritable value : values) {
            pv += value.get ();
        }

        context.write (key, new IntWritable (pv));
    }
}
