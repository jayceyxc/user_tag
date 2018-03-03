package com.bcdata.analysis.yinni;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

public class Utils {
    private static final Logger logger = Logger.getLogger (Utils.class);

    public static final String DATA_FILE_PATH = "data";
    public static final String URL_TAGS_FILE_NAME = "url_tags.txt";
    public static final String URL_TAGS_FILE = System.getProperty("user.dir") + System.getProperty("file.separator")
            + DATA_FILE_PATH + System.getProperty("file.separator") + URL_TAGS_FILE_NAME;

    private static final String HTTP_PREFIX = "http://";
    private static final String HTTPS_PREFIX = "https://";
    private static final String WWW_PREFIX = "www.";

    public static String urlFormat(String url) {
        if (url.startsWith(HTTP_PREFIX)) {
            url = url.substring(HTTP_PREFIX.length());
        } else if (url.startsWith(HTTPS_PREFIX)) {
            url = url.substring(HTTPS_PREFIX.length());
        }

        if (url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }

        if (url.startsWith (WWW_PREFIX)) {
            url = url.substring (WWW_PREFIX.length ());
        }

        return url;
    }

    public static String urlRemoveParams(String urlWithParam) {
        return urlWithParam.split("\\?")[0];
    }

    public static String urlToHost(String url) {
        if (url.startsWith(HTTP_PREFIX)) {
            url = url.substring(HTTP_PREFIX.length());
        } else if (url.startsWith(HTTPS_PREFIX)) {
            url = url.substring(HTTPS_PREFIX.length());
        }
        String urlHost = url.split("/")[0];

        return urlHost;
    }

    public static AhoCorasickDoubleArrayTrie<List<String>> buildACMachine(String filename) {
        try {

            TreeMap<String, List<String>> map = new TreeMap<String, List<String>>();
            BufferedReader reader = new BufferedReader (new FileReader (filename));
            String line = null;
            while ((line = reader.readLine ()) != null) {
                line = line.trim ();
                System.out.println (line);
                int index = StringUtils.lastIndexOf (line, ":");
                if (index > 0) {
                    String urlPattern = urlFormat (line.substring (0, index).trim ());
                    line = line.substring (index + 1);
                    String[] tags = line.trim ().split (",");
                    System.out.println ("put url: " + urlPattern);
                    map.put (urlPattern, Arrays.asList (tags));
                }
            }
            AhoCorasickDoubleArrayTrie<List<String>> acdat = new AhoCorasickDoubleArrayTrie<List<String>> ();
            acdat.build (map);

            return acdat;
        } catch (FileNotFoundException fnfe) {
            logger.error ("File not found");
        } catch (IOException ioe) {
            logger.error ("Read file failed");
        }

        return null;
    }

    public static void main (String[] args) {
        PropertyConfigurator.configure ("conf/log4j.properties");
        // Collect test data set
        TreeMap<String, String> map = new TreeMap<String, String>();
        String[] keyArray = new String[]
                {
                        "hers",
                        "his",
                        "she",
                        "he"
                };
        for (String key : keyArray)
        {
            map.put(key, key);
        }
        // Build an AhoCorasickDoubleArrayTrie
        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<String>();
        acdat.build(map);
        // Test it
        final String text = "uhers";
        List<AhoCorasickDoubleArrayTrie<String>.Hit<String>> wordList = acdat.parseText(text);
        for (AhoCorasickDoubleArrayTrie<String>.Hit<String> hit : wordList) {
            System.out.println ("begin: " +hit.begin + ", end: " + hit.end + ", value: " + hit.value);
        }

        String url = "http://www.baidu.com";
        System.out.println (urlFormat (url));
        url = "https://mail.163.com";
        System.out.println (urlFormat (url));

        logger.info ("begin buildACMachine");
        AhoCorasickDoubleArrayTrie<List<String>> ac = buildACMachine(URL_TAGS_FILE);
        logger.info ("finish buildACMachine");
        url = "http://ad.afy11.net/aaa.html";
        List<AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>>> tagList = ac.parseText (url);
        if (tagList != null) {
            for (AhoCorasickDoubleArrayTrie<List<String>>.Hit<List<String>> hit : tagList) {
                for (String tag : hit.value) {
                    System.out.println ("matched tag: " + tag);
                }
            }
        }
    }
}
