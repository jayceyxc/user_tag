package com.bcdata.analysis.yinni;

import org.apache.hadoop.fs.Path;
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

public class UserTagMain {
    private static final Logger logger = Logger.getLogger (UserTagMain.class);

    /*
    public static class UserTagMap extends Mapper<Object, Text, Text, Text> {
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

            Set<String> finalTagSet = new TreeSet<String>();
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

    public static class UserTagReducer extends Reducer<Text, Text, Text, Text> {
        private static final String SEPERATOR = "\t";
        private static final BigDecimal PRESERVE_RATE = BigDecimal.valueOf (0.7);
        private static final BigDecimal MIN_WEIGHT = BigDecimal.valueOf (0.05);
        private static final int MAX_TAG_NUMBER = 20;

        private static void normalization(Map<String, BigDecimal> tagValueMap) {
            BigDecimal sum = BigDecimal.ZERO;
            for (BigDecimal weight : tagValueMap.values ()) {
                sum = sum.add (weight);
            }

            if (sum.compareTo (BigDecimal.ZERO) > 0) {
                //这里将map.entrySet()转换成list
                List<Map.Entry<String,BigDecimal>> list = new ArrayList<Map.Entry<String,BigDecimal>> (tagValueMap.entrySet());

                //然后通过比较器来实现排序
                list.sort(new Comparator<Map.Entry<String, BigDecimal>> () {
                    // 按value值降序排序，如果要升序，将o2和o1位置互换
                    @Override
                    public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                        return o2.getValue ().compareTo (o1.getValue ());
                    }
                });

                for(Map.Entry<String,BigDecimal> mapping:list){
                    tagValueMap.put (mapping.getKey (), BigDecimal.valueOf (Math.sqrt (mapping.getValue ().doubleValue ())).divide (sum, 2, BigDecimal.ROUND_HALF_UP));
                }
            }
        }

        private static void mergeUserTagMap(Map<String, BigDecimal> curTagValueMap, Map<String, BigDecimal> prevTagValueMap) {
            for(String tag : prevTagValueMap.keySet ()) {
                if (curTagValueMap.containsKey (tag)) {
                    curTagValueMap.put (tag, curTagValueMap.get (tag).add (prevTagValueMap.get (tag).multiply (PRESERVE_RATE)));
                } else {
                    curTagValueMap.put(tag, prevTagValueMap.get (tag).multiply (PRESERVE_RATE));
                }
            }
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String currentUserId = null;
            Map<String, Map<String, BigDecimal>> prevTagMap = new HashMap<String, Map<String, BigDecimal>> ();
            Map<String, Map<String, BigDecimal>> currentTagMap  = new HashMap<String, Map<String, BigDecimal>> ();
            for (Text val : values) {
                String line = val.toString ();
                String segs[] = line.split ("\t");
                if (segs.length == 2) {
                    String adsl = segs[0].trim ();
                    String tagsValues = segs[1].trim ();

                    if (currentUserId == null) {
                        if (tagsValues.contains (":")) {
                            String[] tagValueArray = tagsValues.split (",");
                            for (String tagValue : tagValueArray) {
                                String[] tagValuePair = tagValue.split (":");
                                String tag = tagValuePair[0];
                                BigDecimal tagWeight = BigDecimal.valueOf(Double.valueOf(tagValuePair[1]));
                                prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                            }
                            currentUserId = adsl;
                        } else {
                            Map<String, BigDecimal>  tagValueMap = new HashMap<String, BigDecimal> ();
                            tagValueMap.put (tagsValues, BigDecimal.ONE);
                            currentTagMap.put (adsl, tagValueMap);
                        }
                    } else if (adsl.equals (currentUserId)) {
                        if (tagsValues.contains (":")) {
                            String[] tagValueArray = tagsValues.split (",");
                            for (String tagValue : tagValueArray) {
                                String[] tagValuePair = tagValue.split (":");
                                String tag = tagValuePair[0];
                                BigDecimal tagWeight = BigDecimal.valueOf(Double.valueOf(tagValuePair[1]));
                                prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                            }
                        } else {
                            if (currentTagMap.containsKey (adsl)) {
                                Map<String, BigDecimal> tagValueMap = currentTagMap.get (adsl);
                                if (tagValueMap.containsKey (tagsValues)) {
                                    tagValueMap.put (tagsValues, tagValueMap.get (tagsValues).add (BigDecimal.ONE));
                                }
                            } else {
                                Map<String, BigDecimal>  tagValueMap = new HashMap<String, BigDecimal> ();
                                tagValueMap.put (tagsValues, BigDecimal.ONE);
                                currentTagMap.put (adsl, tagValueMap);
                            }
                        }
                    } else {
                        if (currentTagMap.containsKey (currentUserId)) {
                            normalization (currentTagMap.get (currentUserId));
                            if (prevTagMap.containsKey (currentUserId)) {
                                mergeUserTagMap (currentTagMap.get (currentUserId), prevTagMap.get (currentUserId));
                                normalization (currentTagMap.get (currentUserId));
                            }
                        } else {
                            currentTagMap.put (currentUserId, prevTagMap.get (currentUserId));
                        }
                        int count = 0;
                        //这里将map.entrySet()转换成list
                        List<Map.Entry<String,BigDecimal>> list = new ArrayList<Map.Entry<String,BigDecimal>> (currentTagMap.get (currentUserId).entrySet());
                        //然后通过比较器来实现排序
                        list.sort(new Comparator<Map.Entry<String, BigDecimal>> () {
                            // 按value值降序排序，如果要升序，将o2和o1位置互换
                            @Override
                            public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                                return o2.getValue ().compareTo (o1.getValue ());
                            }
                        });

                        StringBuilder sb = new StringBuilder ();
                        for(Map.Entry<String,BigDecimal> mapping:list){
                            if (mapping.getValue ().compareTo (MIN_WEIGHT) < 0 || count >= MAX_TAG_NUMBER) {
                                break;
                            }

                            if (sb.length () == 0) {
                                sb.append (String.format ("%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
                            } else {
                                sb.append (String.format (",%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
                            }
                            count += 1;
                        }
                        context.write (new Text(currentUserId), new Text(sb.toString ()));
                        currentTagMap.clear ();
                        prevTagMap.clear ();

                        if (tagsValues.contains (":")) {
                            String[] tagValueArray = tagsValues.split (",");
                            for (String tagValue : tagValueArray) {
                                String[] tagValuePair = tagValue.split (":");
                                String tag = tagValuePair[0];
                                BigDecimal tagWeight = BigDecimal.valueOf(Double.valueOf(tagValuePair[1]));
                                prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                            }
                            currentUserId = adsl;
                        } else {
                            Map<String, BigDecimal>  tagValueMap = new HashMap<String, BigDecimal> ();
                            tagValueMap.put (tagsValues, BigDecimal.ONE);
                            currentTagMap.put (adsl, tagValueMap);
                        }
                    }
                }
            }

            if (currentUserId != null) {
                if (currentTagMap.containsKey (currentUserId)) {
                    normalization (currentTagMap.get (currentUserId));
                    if (prevTagMap.containsKey (currentUserId)) {
                        mergeUserTagMap (currentTagMap.get (currentUserId), prevTagMap.get (currentUserId));
                        normalization (currentTagMap.get (currentUserId));
                    }
                } else {
                    currentTagMap.put (currentUserId, prevTagMap.get (currentUserId));
                }

                int count = 0;
                //这里将map.entrySet()转换成list
                List<Map.Entry<String,BigDecimal>> list = new ArrayList<Map.Entry<String,BigDecimal>> (currentTagMap.get (currentUserId).entrySet());
                //然后通过比较器来实现排序
                list.sort(new Comparator<Map.Entry<String, BigDecimal>> () {
                    // 按value值降序排序，如果要升序，将o2和o1位置互换
                    @Override
                    public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                        return o2.getValue ().compareTo (o1.getValue ());
                    }
                });

                StringBuilder sb = new StringBuilder ();
                for(Map.Entry<String,BigDecimal> mapping:list){
                    if (mapping.getValue ().compareTo (MIN_WEIGHT) < 0 || count >= MAX_TAG_NUMBER) {
                        break;
                    }

                    if (sb.length () == 0) {
                        sb.append (String.format ("%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
                    } else {
                        sb.append (String.format (",%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
                    }
                    count += 1;
                }
                context.write (new Text(currentUserId), new Text(sb.toString ()));
            }
        }
    }
    */


    public static void main (String[] args) {
//        PropertyConfigurator.configure ("log4j.properties");

        try {
            JobConf jobConf = new JobConf ();
            jobConf.setNumMapTasks (10);
            jobConf.setNumReduceTasks (10);
//            Configuration conf = new Configuration ();
//            conf.set ("fs.defaultFS", "hdfs://192.168.3.110:8020");
//            conf.addResource (IOUtils.toInputStream (FileUtils.readFileToString (new File ("url_tags.txt"), "utf8")));
            String[] otherArgs = new GenericOptionsParser (jobConf, args).getRemainingArgs ();
            if (otherArgs.length < 2) {
                System.err.println ("Usage: user tag <in> [<in>...] <out>");
                System.exit (2);
            }
            Job job = Job.getInstance (jobConf, "user tag");
            job.setJarByClass (UserTagMain.class);
            job.setMapperClass (UserTagMap.class);
//            job.setCombinerClass (DPCReducer.class);
            job.setReducerClass (UserTagReducer.class);
            job.setMapOutputKeyClass (Text.class);
            job.setMapOutputValueClass (Text.class);
            job.setOutputKeyClass (Text.class);
            job.setOutputValueClass (Text.class);


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
