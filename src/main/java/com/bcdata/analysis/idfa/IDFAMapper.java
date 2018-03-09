package com.bcdata.analysis.idfa;

import com.bcdata.analysis.utils.Utils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IDFAMapper extends Mapper<Object, Text, Text, Text> {

    private final static String SEPERATOR = "\u0001";
    private Text mapKeyword = new Text("url");
    private Text mapValue = new Text();
    private final static int MIN_SEGS_LENGTH = 7;

    private static final int ADSL_INDEX = 0;
    private static final int IP_INDEX = 1;
    private static final int URL_INDEX = 2;
    private static final int REFER_INDEX = 3;
    private static final int COOKIE_INDEX = 5;

    private static final String IDFA_PATTERN = "^(?![0-]{36})[A-Za-z\\d-]{36}$";
    private static Pattern pattern = Pattern.compile (IDFA_PATTERN);

    @Override
    protected void map (Object key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit ();
        String fileName = fileSplit.getPath ().getName ();
        System.out.println (fileName);

        String[] segs = value.toString().split(SEPERATOR);
        if (segs.length < MIN_SEGS_LENGTH) {
            return;
        }

        String adsl = segs[ADSL_INDEX].trim ();
        String sourceIp = segs[IP_INDEX];
        String url = Utils.urlFormat (segs[URL_INDEX].trim ());
        String refer = Utils.urlFormat (segs[REFER_INDEX].trim ());
        String cookie = segs[COOKIE_INDEX].trim ();


        if (adsl.isEmpty ()) {
            adsl = sourceIp;
        }

        Map<String, String> urlParams = new HashMap<> ();
        Map<String, String> cookieParams = new HashMap<> ();
        Utils.parseUrl (url, urlParams);
        Utils.parseCookie (cookie, cookieParams);
        urlParams.putAll (cookieParams);
        for (Map.Entry<String, String> entry : urlParams.entrySet ()) {
            if (entry.getKey ().equals ("idfa")) {
                if (pattern.matcher (entry.getValue ()).matches ()) {
                    mapKeyword.set (adsl);
                    mapValue.set (entry.getValue ());
                    context.write (mapKeyword, mapValue);
                }
            }
        }
    }

    public static void main (String[] args) {
        Pattern pattern = Pattern.compile (IDFA_PATTERN);
        Matcher matcher = pattern.matcher ("halfsdfsdfhalfsdfsdfhalfsdfsdfsdfsdf");
        System.out.println (matcher.matches ());
        matcher = pattern.matcher ("000000000000000000000000000000000000");
        System.out.println (matcher.matches ());
        matcher = pattern.matcher ("A5176B4B-950B-4237-BFCD-3110F658975E");
        System.out.println (matcher.matches ());
    }
}
