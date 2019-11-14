## shell脚本
* shift命令用法，去掉一个参数
* 多行输入

        ssh root@192.168.1.11 <<EOF
            echo "hello" >> test.txt
            cat test.txt
        EOF
* 手动添加classpath属性可以代替jar包的引入