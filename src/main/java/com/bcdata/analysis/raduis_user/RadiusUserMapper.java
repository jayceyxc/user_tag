package com.bcdata.analysis.raduis_user;

import com.bcdata.analysis.utils.Utils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class RadiusUserMapper extends Mapper<Object, Text, CompositeKey, IntWritable> {

    private final static String SEPERATOR = "\u0001";

    private final static IntWritable one = new IntWritable(1);
    private final static int MIN_SEGS_LENGTH = 15;

    private static final int ADSL_INDEX = 4;
    private static final int IP_INDEX = 5;

    @Override
    protected void map (Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit ();
        String fileName = fileSplit.getPath ().getName ();
        String[] tokens = fileName.split ("_");
        String dasName = tokens[0];
//        System.out.println ("input file: " + fileName + ", dasName: " + dasName);

        String[] segs = value.toString().split(SEPERATOR);
        if (segs.length < MIN_SEGS_LENGTH) {
            return;
        }

        String adsl = segs[ADSL_INDEX].trim ();
        String sourceIp = segs[IP_INDEX];


        if (adsl.isEmpty ()) {
            adsl = sourceIp;
        }

        context.write(new CompositeKey (dasName, adsl), one);

        if (dasName.equals ("das1") || dasName.equals ("das2")) {
            context.write (new CompositeKey ("das12", adsl), one);
        } else if (dasName.equals ("das3") || dasName.equals ("das4")) {
            context.write (new CompositeKey ("das34", adsl), one);
        }
    }
}
