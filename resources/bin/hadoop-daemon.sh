#!/usr/bin/env bash

usage="Usage: hadoop-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <hadoop-command> <args...>"

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

# start，stop
startStop=$1
shift
# 进程：namenode，datanode，secondarynamenode，jobtracker，tasktracker，balancer，historyserver
command=$1
shift

# 日志回滚
hadoop_rotate_log ()
{
    log=$1;
    num=5;
    if [[ -n "$2" ]]; then
	    num=$2
    fi
    if [[ -f "$log" ]]; then # rotate logs
        while [[ ${num} -gt 1 ]]; do
            prev=`expr ${num} - 1`
            [[ -f "$log.$prev" ]] && mv "$log.$prev" "$log.$num"
            num=${prev}
        done
        mv "$log" "$log.$num";
    fi
}

# 加载环境变量
if [[ -f "${HADOOP_CONF_DIR}/hadoop-env.sh" ]]; then
  . "${HADOOP_CONF_DIR}/hadoop-env.sh"
fi

# $EUID未知，$HADOOP_SECURE_DN_USER长度非0，默认未设置
if [[ "$command" == "datanode" ]] && [[ "$EUID" -eq 0 ]] && [[ -n "$HADOOP_SECURE_DN_USER" ]]; then
  export HADOOP_PID_DIR=${HADOOP_SECURE_DN_PID_DIR}
  export HADOOP_LOG_DIR=${HADOOP_SECURE_DN_LOG_DIR}
  export HADOOP_IDENT_STRING=${HADOOP_SECURE_DN_USER}
  starting_secure_dn="true"
fi

# $USER当前linux用户
if [[ "$HADOOP_IDENT_STRING" = "" ]]; then
  export HADOOP_IDENT_STRING="$USER"
fi

# 日志目录
if [[ "$HADOOP_LOG_DIR" = "" ]]; then
  export HADOOP_LOG_DIR="$HADOOP_HOME/logs"
fi
mkdir -p "$HADOOP_LOG_DIR"

# 测试当前用户是否有logs目录的权限
touch ${HADOOP_LOG_DIR}/.hadoop_test > /dev/null 2>&1
# $?=0表示上面的命令执行成功，非0表示执行失败
TEST_LOG_DIR=$?
if [[ "${TEST_LOG_DIR}" = "0" ]]; then
  rm -f ${HADOOP_LOG_DIR}/.hadoop_test
else
  chown ${HADOOP_IDENT_STRING} ${HADOOP_LOG_DIR}
fi
# 进程pid文件所在目录
if [[ "$HADOOP_PID_DIR" = "" ]]; then
  HADOOP_PID_DIR="$HADOOP_HOME/tmp/pids"
fi

# 日志文件 hadoop-$USER-进程名-hostname.log
export HADOOP_LOGFILE=hadoop-${HADOOP_IDENT_STRING}-${command}-$HOSTNAME.log
export HADOOP_ROOT_LOGGER="INFO,DRFA"
log=${HADOOP_LOG_DIR}/hadoop-${HADOOP_IDENT_STRING}-${command}-$HOSTNAME.out
pid=${HADOOP_PID_DIR}/hadoop-${HADOOP_IDENT_STRING}-${command}.pid
HADOOP_STOP_TIMEOUT=${HADOOP_STOP_TIMEOUT:-5}

# 默认优先级
if [[ "$HADOOP_NICENESS" = "" ]]; then
    export HADOOP_NICENESS=0
fi

case ${startStop} in

  (start)

    mkdir -p "$HADOOP_PID_DIR"
    # pid文件已存在
    if [[ -f ${pid} ]]; then
      # 检查pid进程是否存在
      if kill -0 `cat ${pid}` > /dev/null 2>&1; then
        echo ${command} running as process `cat ${pid}`.  Stop it first.
        exit 1
      fi
    fi

    # HADOOP_MASTER默认为空
    if [[ "$HADOOP_MASTER" != "" ]]; then
      echo rsync from ${HADOOP_MASTER}
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' ${HADOOP_MASTER}/ "$HADOOP_HOME"
    fi

    # 日志回滚
    hadoop_rotate_log ${log}
    echo starting ${command}, logging to ${log}
    cd "$HADOOP_PREFIX"
    nohup nice -n ${HADOOP_NICENESS} "$HADOOP_PREFIX"/bin/hadoop --config ${HADOOP_CONF_DIR} ${command} "$@" > "$log" 2>&1 < /dev/null &
    echo $! > ${pid}
    head "$log"
    # capture the ulimit output
    if [[ "true" = "$starting_secure_dn" ]]; then
      echo "ulimit -a for secure datanode user $HADOOP_SECURE_DN_USER" >> ${log}
      # capture the ulimit info for the appropriate user
      su --shell=/bin/bash ${HADOOP_SECURE_DN_USER} -c 'ulimit -a' >> ${log} 2>&1
    else
      echo "ulimit -a for user $USER" >> ${log}
      ulimit -a >> ${log} 2>&1
    fi
    ;;
          
  (stop)

    if [[ -f ${pid} ]]; then
      TARGET_PID=`cat ${pid}`
      if kill -0 ${TARGET_PID} > /dev/null 2>&1; then
        echo stopping ${command}
        kill ${TARGET_PID} #杀掉进程
        # 休息默认5秒
        sleep ${HADOOP_STOP_TIMEOUT}
        if kill -0 ${TARGET_PID} > /dev/null 2>&1; then
          # 如果休息5秒进程还在
          echo "$command did not stop gracefully after $HADOOP_STOP_TIMEOUT seconds: killing with kill -9"
          # 强制杀掉
          kill -9 ${TARGET_PID}
        fi
      else
        echo no ${command} to stop
      fi
    else
      echo no ${command} to stop
    fi
    ;;

  (*)
    echo ${usage}
    exit 1
    ;;

esac


