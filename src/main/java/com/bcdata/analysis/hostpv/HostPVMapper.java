package com.bcdata.analysis.hostpv;

import com.bcdata.analysis.utils.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class HostPVMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static String SEPERATOR = "\u0001";

    private final static IntWritable one = new IntWritable(1);
    private final static int MIN_SEGS_LENGTH = 7;

    private static final int URL_INDEX = 2;

    @Override
    protected void map (Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] segs = value.toString().split(SEPERATOR);
        if (segs.length < MIN_SEGS_LENGTH) {
            return;
        }

        String url = Utils.urlFormat (segs[URL_INDEX].trim ());
        String host = Utils.urlToHost (url);

        context.write(new Text(host), one);
    }
}
