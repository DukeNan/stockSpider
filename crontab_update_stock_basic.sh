#!/usr/bin/env bash
# 定时更新公司信息表 0 3,6 * * *
home=$(cd $(dirname $0);pwd)

cd $home

ts=`date +%Y%m%d`

file="loop_update_stock_basic.py"

if [ $# -eq 1 ]; then
    ps -ef | grep "$home/$file" | grep -v "grep" | awk '{print $2}' | while read pid; do kill -9 $pid; done
    sleep 1
fi

cnt=$(ps -ef | grep  "$home/$file" | grep -v "grep"| wc -l)

if [ $cnt -eq 0 ];then
    $home/.venv/bin/python3 $home/$file >> ./log/${file}.${ts}.log 2>&1 &
fi

