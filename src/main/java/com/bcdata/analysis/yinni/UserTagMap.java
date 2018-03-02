package com.bcdata.analysis.yinni;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class UserTagMap extends Mapper<Object, Text, Text, Text> {
    private final static String SEPERATOR = "\u0001";
    private Text mapKeyword = new Text("url");
    private Text mapValue = new Text();
    private final static IntWritable one = new IntWritable(1);
    private final static int MIN_SEGS_LENGTH = 7;

    private static final int ADSL_INDEX = 0;
    private static final int IP_INDEX = 1;
    private static final int URL_INDEX = 2;
    private static final int REFER_INDEX = 3;
    private static final int UA_INDEX = 4;
    private static final int COOKIE_INDEX = 5;
    private static final int TIMESTAMP_INDEX = 6;

    private static final AhoCorasickDoubleArrayTrie<List<String>> ac = Utils.buildACMachine ();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] segs = value.toString().split(SEPERATOR);
        if (segs.length < MIN_SEGS_LENGTH) {
            segs = value.toString ().split ("\t");
            if (segs.length == 2) {
                mapKeyword = new Text(segs[0]);
                mapValue = new Text(segs[1]);
                context.write (mapKeyword, mapValue);
            }
            return;
        }
        String adsl = segs[ADSL_INDEX];
        String sourceIp = segs[IP_INDEX];
        String url = Utils.urlFormat (segs[URL_INDEX].trim ());
        String refer = Utils.urlFormat (segs[REFER_INDEX].trim ());
        String userAgent = segs[UA_INDEX];
        String timestamp = segs[TIMESTAMP_INDEX];


        if (adsl.isEmpty ()) {
            adsl = sourceIp;
        }

        Set<String> finalTagSet = new TreeSet<String> ();
        List<AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>>> tagList = ac.parseText (url);
        if (tagList != null) {
            for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                for (String tag : hit.value) {
                    finalTagSet.add (tag);
                }
            }
        }

        tagList.clear ();
        tagList = ac.parseText (refer);
        if (tagList != null) {
            for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                for (String tag : hit.value) {
                    finalTagSet.add (tag);
                }
            }
        }

        tagList.clear ();
        String host = Utils.urlToHost (url);
        tagList = ac.parseText (host);
        if (tagList != null) {
            for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                for (String tag : hit.value) {
                    finalTagSet.add (tag);
                }
            }
        }

        tagList.clear ();
        String referHost = Utils.urlToHost (refer);
        tagList = ac.parseText (referHost);
        if (tagList != null) {
            for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                for (String tag : hit.value) {
                    finalTagSet.add (tag);
                }
            }
        }
        for (String tag : finalTagSet) {
            context.write (new Text(adsl), new Text(tag));
        }
    }
}
