# esq

## 这是个什么

轻量级消息队列，是对 <https://github.com/wuzhc/gmq> 的魔改，看看正经的轻量级消息队列应该如何设计，大有裨益。

## 使用方法

### 启动 esq 的单个节点

``` sh
cd cmd/gnode

go build

./gnode -http_addr=":9504" -tcp_addr=":9503" -etcd_endpoints="127.0.0.1:2379" -node_id=1 -node_weight=1
```

#### 简单的客户端测试 —— 发送与接受心跳

``` sh
cd cmd/singleCli

go run ./main.go
```

### 启动 esq 集群

需要安装 etcd 并运行
``` sh
etcd
```


``` sh
cd cmd/gnode

go build

# 节点1
./gnode -http_addr="127.0.0.1:9504" -tcp_addr="127.0.0.1:9503" -etcd_endpoints="127.0.0.1:2379" -node_id=1 -node_weight=1 -data_save_path=./data1 -enable_cluster=true

# 节点2
./gnode -http_addr="127.0.0.1:9506" -tcp_addr="127.0.0.1:9505" -etcd_endpoints="127.0.0.1:2379" -node_id=2 -node_weight=2 -data_save_path=./data2 -enable_cluster=true
```


#### 简单的客户端测试 —— 发送与接受心跳

``` sh
cd cmd/clusterCli

# 1 2 3 对应三种不同的选择节点的方式
go run ./main.go 1
go run ./main.go 2
go run ./main.go 3
```

### 客户端源码

- 单节点 <https://github.com/impact-eintr/esq/blob/main/cmd/singleCli/main.go>
- 集群模式 <https://github.com/impact-eintr/esq/blob/main/cmd/clusterCli/main.go>

## 说点什么

感谢伟大的开源运动，让我能看到这么多优秀的前辈留下的代码，给我这个菜鸡开个大眼。
