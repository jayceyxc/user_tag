package com.bcdata.analysis.hostpv;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HostPVDriver {

    private static final Logger logger = Logger.getLogger (HostPVDriver.class);

    public static void main (String[] args) {
//        PropertyConfigurator.configure ("log4j.properties");

        try {
            JobConf jobConf = new JobConf ();
            jobConf.setNumMapTasks (20);
            jobConf.setNumReduceTasks (20);
//            Configuration conf = new Configuration ();
//            conf.set ("fs.defaultFS", "hdfs://192.168.3.110:8020");
//            conf.addResource (IOUtils.toInputStream (FileUtils.readFileToString (new File ("url_tags.txt"), "utf8")));
            String[] otherArgs = new GenericOptionsParser (jobConf, args).getRemainingArgs ();
            if (otherArgs.length < 2) {
                System.err.println ("Usage: user tag <in> [<in>...] <out>");
                System.exit (2);
            }
            Job job = Job.getInstance (jobConf, "host pv");
            job.setJarByClass (HostPVDriver.class);
            job.setMapperClass (HostPVMapper.class);
            job.setCombinerClass (HostPVReducer.class);
            job.setReducerClass (HostPVReducer.class);
            job.setMapOutputKeyClass (Text.class);
            job.setMapOutputValueClass (IntWritable.class);
            job.setOutputKeyClass (Text.class);
            job.setOutputValueClass (IntWritable.class);


            List<String> inputPaths = new ArrayList<String> ();
            for (int i = 0; i < otherArgs.length - 1; ++i) {
                System.out.println ("input file path: " + otherArgs[i]);
                inputPaths.add (otherArgs[i]);
            }

            FileInputFormat.setInputPaths (job, String.join (",", inputPaths));

            String outfilePath = otherArgs[otherArgs.length - 1];
//            OperatingFiles.deleteHDFSFile(outfilePath);

            System.out.println ("outfilePath: " + outfilePath);
            FileOutputFormat.setOutputPath (job, new Path (outfilePath));
            job.waitForCompletion (true);
            System.out.println ("Job ended: ");
        } catch (ClassNotFoundException cnfe) {
            logger.error (cnfe.getMessage (), cnfe);
        } catch (IOException ioe) {
            logger.error (ioe.getMessage (), ioe);
        } catch (InterruptedException ie) {
            logger.error (ie.getMessage (), ie);
        }
    }
}
