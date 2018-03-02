#!/bin/sh

hadoop jar user_tag.jar com.bcdata.analysis.yinni.UserTagMain -files url_tags.txt -libjars aho-corasick-double-array-trie-1.1.0.jar  yinni_visit/20180202/* yinni_user_tag/20180202
