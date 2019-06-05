#!/bin/sh

JOB_DAY=$1
OUT_PATH=$2
IN_PATH=$3

OUT_PATH=${OUT_PATH}/${JOB_DAY}
hadoop fs -rm -r -skipTrash ${OUT_PATH}

echo ${IN_PATH} ${OUT_PATH}

hadoop jar user_tag.jar com.bcdata.analysis.raduis_user.RadiusUserDriver -libjars aho-corasick-double-array-trie-1.1.0.jar,commons-lang3-3.7.jar,commons-text-1.2.jar  ${IN_PATH} ${OUT_PATH}

echo "Totally ${JOB_DAY} done!!"
exit $?