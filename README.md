# 测试
功能测试使用类： MySqlSyncDatabaseExtActionITCaseNoDocker
配置启动的测试类：ActionRunTest.test01()
# mysql命令
binlog 太大不好测试就用 flush logs;  滚动一下
PURGE BINARY LOGS TO 'binlog.000011';
删除历史的binlog
# 问题
## 时区问题验证
timestamp类型没问题。spark读取没问题，但是trino读取出来的有问题。


