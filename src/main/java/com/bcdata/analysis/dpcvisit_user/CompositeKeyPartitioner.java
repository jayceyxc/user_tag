package com.bcdata.analysis.dpcvisit_user;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * CompositeKeyPartitioner
 *
 * This custom partitioner allow us to distribute how outputs from the
 * map stage are sent to the reducers.  CompositeKeyPartitioner partitions
 * the data output from the map phase (DPCVisitUserMapper) before it is
 * sent through the shuffle phase. Since we want a single reducer to receive
 * all time series data for a single "das", we partition data output
 * of the map phase by only the composite key component ("das").
 *
 *
 */
public class CompositeKeyPartitioner extends Partitioner<CompositeKey, IntWritable> {
    @Override
    public int getPartition (CompositeKey compositeKey, IntWritable intWritable, int numPartitions) {
//        System.out.println ("total partitions: " + numPartitions + ", my partition: " +  Math.abs ((int) (hash (compositeKey.getDas ()) % numPartitions)));
        return Math.abs ((int) (hash (compositeKey.getDas ()) % numPartitions));
    }

    /**
     *  adapted from String.hashCode()
     */
    static long hash(String str) {
        long h = 1125899906842597L; // prime
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31*h + str.charAt(i);
        }
        return h;
    }
}
