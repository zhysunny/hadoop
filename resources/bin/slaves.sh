#!/usr/bin/env bash

usage="Usage: slaves.sh [--config confdir] command..."

# 没参数直接返回
if [[ $# -le 0 ]]; then
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

# 找到slaves文件
if [[ "$HOSTLIST" = "" ]]; then
  if [[ "$HADOOP_SLAVES" = "" ]]; then
    export HOSTLIST="${HADOOP_CONF_DIR}/slaves"
  else
    export HOSTLIST="${HADOOP_SLAVES}"
  fi
fi
# 循环slaves文件，去掉注释的行
for slave in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh ${HADOOP_SSH_OPTS} ${slave} $"${@// /\\ }" \
   2>&1 | sed "s/^/$slave: /" &
 if [[ "$HADOOP_SLAVE_SLEEP" != "" ]]; then
   sleep ${HADOOP_SLAVE_SLEEP}
 fi
done

wait
