# kafka-demo
消息中间件kafka的demo测试


####安装zookeeper
mac安装方法:

```
brew install zookeeper 
```

mac启动方法:

```
zkServer start
```

####安装kafka

mac安装方法:

```
brew install kafka
```

mac启动方法:

```
kafka-server-start.sh /usr/local/etc/kafka/server.properties
```

####程序测试方法:

Producer:

```
com.sinosoft.mq.ProducerThread
```

Consumer:

```
com.sinosoft.mq.ConsumerTask
```

