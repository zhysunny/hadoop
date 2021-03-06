#!/usr/bin/env bash

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
  . ${basedir}/conf/hadoop-config.sh
else
  echo "$basedir/conf/hadoop-config.sh is not found"
  exit 1
fi

"$bin"/hadoop-daemon.sh --config ${HADOOP_CONF_DIR} stop namenode
"$bin"/hadoop-daemons.sh --config ${HADOOP_CONF_DIR} stop datanode
"$bin"/hadoop-daemons.sh --config ${HADOOP_CONF_DIR} --hosts masters stop secondarynamenode

