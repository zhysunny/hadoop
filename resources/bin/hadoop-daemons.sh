#!/usr/bin/env bash

usage="Usage: hadoop-daemons.sh [--config confdir] [--hosts hostlistfile] [start|stop] command args..."

# 没参数直接返回
if [[ $# -le 1 ]]; then
  echo ${usage}
  exit 1
fi

# 当前文件所在目录，这里是相对路径
parent=`dirname $0`
# 当前文件所在目录转为绝对路径
parent=`cd $parent;pwd`
# this为当前文件名
this=`basename $0`
# 当前文件所在目录，一般是安装路径
basedir=`cd ${parent}/../;pwd`
# 当前文件全路径
this=${parent}/${this}
# 当前文件所在目录为bin目录
bin=${parent}
if [[ -e "$basedir/conf/hadoop-config.sh" ]]; then
  # source过程中将--config和--hosts去掉了
  . ${basedir}/conf/hadoop-config.sh
else
  echo "$basedir/conf/hadoop-config.sh is not found"
  exit 1
fi

exec "$bin/slaves.sh" --config ${HADOOP_CONF_DIR} cd "$HADOOP_HOME" \; "$bin/hadoop-daemon.sh" --config ${HADOOP_CONF_DIR} "$@"
