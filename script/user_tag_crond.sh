#!/bin/bash
function log_info ()
{
	DATE_N=`date "+%Y-%m-%d %H:%M:%S"`
	USER_N=`whoami`
	echo "${DATE_N} ${USER_N} execute $0 [INFO] $@"
}

# 需要执行下面这个source语句,因为crontab任务中不会继承.bash_profile,.bash_rc脚本中设置的环境变量,需要自己执行一遍
profile_name=/home/hadoop/.bash_profile
if [ -f ${profile_name} ]; then
	log_info "${profile_name} exists"
	source ${profile_name}
fi

log_info $(date)

day=`date -d "1 hours ago" "+%Y%m%d"`

python run_query_jobs.py ${day} > java_job_${day}.log 2>&1 &
