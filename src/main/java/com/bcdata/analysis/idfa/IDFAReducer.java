package com.bcdata.analysis.idfa;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class IDFAReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> idfaSet = new TreeSet<String> ();
        for (Text value : values) {
            idfaSet.add (value.toString ());
        }

        String idfaStr = String.join (",", idfaSet);
        context.write (key, new Text(idfaStr));
    }
}
