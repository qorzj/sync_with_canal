# sync_with_canal
基于canal和python的数据库同步工具

```
Usage:     python start.py [options]

Options:
  -h, --help            show this help message and exit
  -v, --verbose         输出调试信息
  -w, --write           指定是否写入dest_mysql，不指定则只消费队列
  -c CONFIG_FILE, --config=CONFIG_FILE
                        指定配置文件路径
```

配置项：
* DATABASES  例如：db1,db2
* CANAL_HOST  例如：127.0.0.1
* CANAL_PORT  例如：11111
* CANAL_USERNAME
* CANAL_PASSWORD
* CANAL_CLIENT_ID
* CANAL_DESTINATION
* DEST_MYSQL_HOST
* DEST_MYSQL_PORT
* DEST_MYSQL_USERNAME
* DEST_MYSQL_PASSWORD
* ERROR_LOG_PATH
