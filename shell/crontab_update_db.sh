#!/usr/bin/env bash
# 定时更新公司信息表 0 3,6 * * *
# 进入项目根目录 stockSpider
home=$(dirname $(cd $(dirname $0);pwd))


#cd $home

ts=`date +%Y%m%d`


file="update_db.py"

log_file=$home/logs/${file}.${ts}.log



if [ $# -eq 1 ]; then
    ps -ef | grep "$home/spider/$file" | grep -v "grep" | awk '{print $2}' | while read pid; do kill -9 $pid; done
    sleep 1
fi

cnt=$(ps -ef | grep  "$home/spider/$file" | grep -v "grep"| wc -l)

if [ $cnt -eq 0 ];then
    cd $home/spider
    $home/.venv/bin/python3 "$home/spider/$file" >> $log_file 2>&1 &
fi

