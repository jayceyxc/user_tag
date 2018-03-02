package com.bcdata.analysis.yinni;

import org.apache.commons.cli.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

public class UserTagReducer extends Reducer<Text, Text, Text, Text> {
    private static final String SEPERATOR = "\t";
    private static final BigDecimal PRESERVE_RATE = BigDecimal.valueOf (0.7);
    private static final BigDecimal MIN_WEIGHT = BigDecimal.valueOf (0.005);
    private static final int MAX_TAG_NUMBER = 20;
    private static String currentUserId = null;
    private static Map<String, Map<String, BigDecimal>> prevTagMap = new HashMap<String, Map<String, BigDecimal>> ();
    private static Map<String, Map<String, BigDecimal>> currentTagMap = new HashMap<String, Map<String, BigDecimal>> ();

    private static void normalization (Map<String, BigDecimal> tagValueMap) {
        BigDecimal sum = BigDecimal.ZERO;
        for (BigDecimal weight : tagValueMap.values ()) {
            sum = sum.add (weight);
        }
        System.err.println (String.format ("map size: %d, sum: %f", tagValueMap.size (), sum.doubleValue ()));

        if (sum.compareTo (BigDecimal.ZERO) > 0) {
            //这里将map.entrySet()转换成list
            List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (tagValueMap.entrySet ());

            //然后通过比较器来实现排序
            list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
                // 按value值降序排序，如果要升序，将o2和o1位置互换
                @Override
                public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                    return o2.getValue ().compareTo (o1.getValue ());
                }
            });

            for (Map.Entry<String, BigDecimal> mapping : list) {
                System.err.println (String.format ("normalization. %s:%f", mapping.getKey(), BigDecimal.valueOf (Math.sqrt (mapping.getValue ().doubleValue ())).divide (sum, 2, BigDecimal.ROUND_HALF_UP).doubleValue ()));
                tagValueMap.put (mapping.getKey (), BigDecimal.valueOf (Math.sqrt (mapping.getValue ().doubleValue ())).divide (sum, 2, BigDecimal.ROUND_HALF_UP));
            }
        }
    }

    private static void mergeUserTagMap (Map<String, BigDecimal> curTagValueMap, Map<String, BigDecimal> prevTagValueMap) {
        for (String tag : prevTagValueMap.keySet ()) {
            if (curTagValueMap.containsKey (tag)) {
                curTagValueMap.put (tag, curTagValueMap.get (tag).add (prevTagValueMap.get (tag).multiply (PRESERVE_RATE)));
            } else {
                curTagValueMap.put (tag, prevTagValueMap.get (tag).multiply (PRESERVE_RATE));
            }
        }
    }

    @Override
    public void reduce (Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text val : values) {
            String adsl = key.toString ();
            String tagsValues = val.toString ();

            if (currentUserId == null) {
                if (tagsValues.contains (":")) {
                    String[] tagValueArray = tagsValues.split (",");
                    for (String tagValue : tagValueArray) {
                        String[] tagValuePair = tagValue.split (":");
                        String tag = tagValuePair[0];
                        BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                        prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                    }
                } else {
                    Map<String, BigDecimal> tagValueMap = new HashMap<String, BigDecimal> ();
                    tagValueMap.put (tagsValues, BigDecimal.ONE);
                    currentTagMap.put (adsl, tagValueMap);
//                    System.err.println ("currentUserId is null, adsl: " + adsl + ", tag: " + tagsValues);
                }
                currentUserId = adsl;
            } else if (adsl.equals (currentUserId)) {
                if (tagsValues.contains (":")) {
                    String[] tagValueArray = tagsValues.split (",");
                    for (String tagValue : tagValueArray) {
                        String[] tagValuePair = tagValue.split (":");
                        String tag = tagValuePair[0];
                        BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                        prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                    }
                } else {
                    if (currentTagMap.containsKey (adsl)) {
                        Map<String, BigDecimal> tagValueMap = currentTagMap.get (adsl);
                        if (tagValueMap.containsKey (tagsValues)) {
                            tagValueMap.put (tagsValues, tagValueMap.get (tagsValues).add (BigDecimal.ONE));
                        }
                    } else {
                        Map<String, BigDecimal> tagValueMap = new HashMap<String, BigDecimal> ();
                        tagValueMap.put (tagsValues, BigDecimal.ONE);
                        currentTagMap.put (adsl, tagValueMap);
                    }
//                    System.err.println ("adsl: " + adsl + ", tag: " + tagsValues);
                }
            } else {
                if (currentTagMap.containsKey (currentUserId)) {
//                    System.err.println ("normalization for adsl: " + currentUserId);
                    normalization (currentTagMap.get (currentUserId));
                    if (prevTagMap.containsKey (currentUserId)) {
//                        System.err.println ("mergeUserTagMap for adsl: " + currentUserId);
                        mergeUserTagMap (currentTagMap.get (currentUserId), prevTagMap.get (currentUserId));
//                        System.err.println ("second normalization for adsl: " + currentUserId);
                        normalization (currentTagMap.get (currentUserId));
                    }
                } else {
//                    System.err.println ("use prevTagMap for adsl: " + currentUserId);
                    currentTagMap.put (currentUserId, prevTagMap.get (currentUserId));
                }
                int count = 0;
                //这里将map.entrySet()转换成list
//                System.err.println ("currentUserId: " + currentUserId);
                Set<Map.Entry<String, BigDecimal>> entries = currentTagMap.get (currentUserId).entrySet ();
                if (entries.isEmpty ()) {
                    System.err.println ("entry set is empty");
                }
                List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (entries);
                //然后通过比较器来实现排序
                list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
                    // 按value值降序排序，如果要升序，将o2和o1位置互换
                    @Override
                    public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                        return o2.getValue ().compareTo (o1.getValue ());
                    }
                });

                StringBuilder sb = new StringBuilder ();
                for (Map.Entry<String, BigDecimal> mapping : list) {
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
                context.write (new Text (currentUserId), new Text (sb.toString ()));
                currentTagMap.clear ();
                prevTagMap.clear ();

                if (tagsValues.contains (":")) {
                    String[] tagValueArray = tagsValues.split (",");
                    for (String tagValue : tagValueArray) {
                        String[] tagValuePair = tagValue.split (":");
                        String tag = tagValuePair[0];
                        BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                        prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                    }
                } else {
                    Map<String, BigDecimal> tagValueMap = new HashMap<String, BigDecimal> ();
                    tagValueMap.put (tagsValues, BigDecimal.ONE);
                    currentTagMap.put (adsl, tagValueMap);
                }
                currentUserId = adsl;
            }
        }

//        if (currentUserId != null) {
//            if (currentTagMap.containsKey (currentUserId)) {
//                normalization (currentTagMap.get (currentUserId));
//                if (prevTagMap.containsKey (currentUserId)) {
//                    mergeUserTagMap (currentTagMap.get (currentUserId), prevTagMap.get (currentUserId));
//                    normalization (currentTagMap.get (currentUserId));
//                }
//            } else {
//                currentTagMap.put (currentUserId, prevTagMap.get (currentUserId));
//            }
//
//            int count = 0;
//            //这里将map.entrySet()转换成list
//            List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (currentTagMap.get (currentUserId).entrySet ());
//            //然后通过比较器来实现排序
//            list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
//                // 按value值降序排序，如果要升序，将o2和o1位置互换
//                @Override
//                public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
//                    return o2.getValue ().compareTo (o1.getValue ());
//                }
//            });
//
//            StringBuilder sb = new StringBuilder ();
//            for (Map.Entry<String, BigDecimal> mapping : list) {
//                if (mapping.getValue ().compareTo (MIN_WEIGHT) < 0 || count >= MAX_TAG_NUMBER) {
//                    break;
//                }
//
//                if (sb.length () == 0) {
//                    sb.append (String.format ("%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
//                } else {
//                    sb.append (String.format (",%s:%2f", mapping.getKey (), mapping.getValue ().doubleValue ()));
//                }
//                count += 1;
//            }
//            context.write (new Text (currentUserId), new Text (sb.toString ()));
//        }
    }

    public static void testReduce (List<String> values) {
        String currentUserId = null;
        Map<String, Map<String, BigDecimal>> prevTagMap = new HashMap<String, Map<String, BigDecimal>> ();
        Map<String, Map<String, BigDecimal>> currentTagMap = new HashMap<String, Map<String, BigDecimal>> ();
        for (String line : values) {
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
                            BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                            prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                        }
                        currentUserId = adsl;
                    } else {
                        Map<String, BigDecimal> tagValueMap = new HashMap<String, BigDecimal> ();
                        tagValueMap.put (tagsValues, BigDecimal.ONE);
                        currentTagMap.put (adsl, tagValueMap);
                    }
                } else if (adsl.equals (currentUserId)) {
                    if (tagsValues.contains (":")) {
                        String[] tagValueArray = tagsValues.split (",");
                        for (String tagValue : tagValueArray) {
                            String[] tagValuePair = tagValue.split (":");
                            String tag = tagValuePair[0];
                            BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                            prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                        }
                    } else {
                        if (currentTagMap.containsKey (adsl)) {
                            Map<String, BigDecimal> tagValueMap = currentTagMap.get (adsl);
                            if (tagValueMap.containsKey (tagsValues)) {
                                tagValueMap.put (tagsValues, tagValueMap.get (tagsValues).add (BigDecimal.ONE));
                            }
                        } else {
                            Map<String, BigDecimal> tagValueMap = new HashMap<String, BigDecimal> ();
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
                    List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (currentTagMap.get (currentUserId).entrySet ());
                    //然后通过比较器来实现排序
                    list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
                        // 按value值降序排序，如果要升序，将o2和o1位置互换
                        @Override
                        public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                            return o2.getValue ().compareTo (o1.getValue ());
                        }
                    });

                    StringBuilder sb = new StringBuilder ();
                    for (Map.Entry<String, BigDecimal> mapping : list) {
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
                    System.out.println (String.format ("%s:%s", currentUserId, sb.toString ()));
                    currentTagMap.clear ();
                    prevTagMap.clear ();

                    if (tagsValues.contains (":")) {
                        String[] tagValueArray = tagsValues.split (",");
                        for (String tagValue : tagValueArray) {
                            String[] tagValuePair = tagValue.split (":");
                            String tag = tagValuePair[0];
                            BigDecimal tagWeight = BigDecimal.valueOf (Double.valueOf (tagValuePair[1]));
                            prevTagMap.getOrDefault (adsl, new HashMap<String, BigDecimal> ()).put (tag, tagWeight);
                        }
                        currentUserId = adsl;
                    } else {
                        Map<String, BigDecimal> tagValueMap = new HashMap<String, BigDecimal> ();
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
            List<Map.Entry<String, BigDecimal>> list = new ArrayList<Map.Entry<String, BigDecimal>> (currentTagMap.get (currentUserId).entrySet ());
            //然后通过比较器来实现排序
            list.sort (new Comparator<Map.Entry<String, BigDecimal>> () {
                // 按value值降序排序，如果要升序，将o2和o1位置互换
                @Override
                public int compare (Map.Entry<String, BigDecimal> o1, Map.Entry<String, BigDecimal> o2) {
                    return o2.getValue ().compareTo (o1.getValue ());
                }
            });

            StringBuilder sb = new StringBuilder ();
            for (Map.Entry<String, BigDecimal> mapping : list) {
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
            System.out.println (String.format ("%s:%s", currentUserId, sb.toString ()));
        }
    }

    public static void main (String[] args) throws FileNotFoundException, IOException {
        Options options = new Options ();
        options.addOption ("f", "file", true, "The file name of map result, required");
        CommandLine cl = null;
        try {
            cl = new DefaultParser ().parse (options, args);
        } catch (ParseException e) {
            e.printStackTrace ();
        }

        if (cl == null || !cl.hasOption ("f") || !cl.hasOption ("file")) {
            new HelpFormatter ().printHelp ("java [options] -f [user_tag_file]", options);
            return;
        }

        String fileName = cl.getOptionValue ("f");
        BufferedReader reader = new BufferedReader (new FileReader (fileName));
        String line = null;
    }
}
