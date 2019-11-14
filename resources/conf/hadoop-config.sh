#!/bin/sh

################################################################## 保留代码 start ##################################################################
# this为当前文件名
this="${BASH_SOURCE-$0}"
# common_bin为当前文件所在的目录，等价于dirname $0
common_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
# script为当前文件名
script="$(basename -- "$this")"
# this为当前文件全路径
this="$common_bin/$script"

# config_bin为当前文件所在的目录，等价于dirname $0
config_bin=`dirname "$this"`
# script为当前文件名
script=`basename "$this"`
# config_bin为当前文件所在的目录
config_bin=`cd "$config_bin"; pwd`
# this为当前文件全路径
this="$config_bin/$script"
################################################################## 保留代码 end ##################################################################

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

export HADOOP_PREFIX=${basedir}

# 指定conf目录路径
if [[ $# -gt 1 ]]
then
    if [[ "--config" = "$1" ]]
	  then
		  # shift命令，$0不动，$1丢弃，$2左移变$1，后面以此类推
	      shift
	      confdir=$1
	      shift
	      HADOOP_CONF_DIR=${confdir}
    fi
fi

# 默认路径
if [[ -e "${HADOOP_PREFIX}/conf/hadoop-env.sh" ]]; then
  DEFAULT_CONF_DIR="conf"
else
  DEFAULT_CONF_DIR="etc/hadoop"
fi
# 没有指定--config时HADOOP_CONF_DIR为空，选择冒号后面的变量
# param=${prefix:-$suffix}
HADOOP_CONF_DIR="${HADOOP_CONF_DIR:-$HADOOP_PREFIX/$DEFAULT_CONF_DIR}"

# 如果hadoop-env.sh文件存在，source一下
if [[ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# 指定从节点文件所在位置，默认在${HADOOP_CONF_DIR}目录下找指定的文件
if [[ $# -gt 1 ]]
then
    if [[ "--hosts" = "$1" ]]
    then
        shift
        slavesfile=$1
        shift
        export HADOOP_SLAVES="${HADOOP_CONF_DIR}/$slavesfile"
    fi
fi
HADOOP_SLAVES="${HADOOP_SLAVES:-${HADOOP_PREFIX}/conf/slaves}"

# MALLOC_ARENA_MAX默认设置为4
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}
# HADOOP_HOME
export HADOOP_HOME=${HADOOP_PREFIX}

