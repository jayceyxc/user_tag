#!/bin/sh

JOB_DAY=$1
OUT_PATH=$2
IN_PATH=$3

OUT_PATH=${OUT_PATH}/${JOB_DAY}
hadoop fs -rm -r -skipTrash ${OUT_PATH}

echo ${IN_PATH} ${OUT_PATH}

hadoop jar user_tag.jar com.bcdata.analysis.yinni.UserTagMain -files url_tags.txt -libjars aho-corasick-double-array-trie-1.1.0.jar  ${IN_PATH} ${OUT_PATH}

echo "Totally ${JOB_DAY} done!!"
exit $?

TABLE=bc_user_tag
hive -e"
use default;
alter table ${TABLE} drop if exists partition (day=$JOB_DAY);
alter table ${TABLE} add if not exists partition (day=$JOB_DAY) location '/user/hadoop/${OUT_PATH}';"

exit $?