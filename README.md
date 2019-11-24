# pyspark_ml
基于pyspark的分类标签抽取与ml库的研究

## 1.spark环境配置与启动
    1.先启动hadoop进程，start-all.sh；
    2.分别启动zookeeper进程，zkServer.sh start；
    3.启动hbase进程，start-hbase.sh；
    4.在zookeeper的bin目录下有一个zkCli.sh ,运行进入；
    5.进入后运行 rmr /hbase , 然后quit退出；
    6.spark-submit提交任务：spark-submit --master yarn --deploy-mode client --num-executors 6 --driver-memory 1g --executor-memory 5g -- executor-cores 2 demo.py
    7.Hadoop的NameNode处在安全模式: bin/hadoop dfsadmin -safemode leave  bin/hadoop dfsadmin -safemode get
    
## 2.配置教程
    http://www.panchengming.com/2017/12/30/pancm64/
    https://zhuanlan.zhihu.com/p/59112774
    
## 3.Python使用Thrift连接HBASE进行操作
    https://blog.csdn.net/m0_37634723/article/details/79191420


