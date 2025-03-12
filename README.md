## 项目介绍

本项目是基于Raft算法的一个简单KV分布式数据库。多节点实现领导选举，日志复制，多节点共识集群变更，快照，WAL等机制。底层数据库基于bbolt进行封装，实现读写缓存，事务批量提交，并发读机制（参考Etcd进行简化）。整体项目实现故障恢复，KV读写，WAL，数据快照功能。具备一定的容灾性，高可用性。 



# 项目结构



—— bench                    //benchmark测试

—— client                     //客户端 

—— conf                       //server配置以及集群变更的新集群配置

——config                     //yaml文件读取

——main                       //包含三个main，clientMain，serverMain，清理数据文件的cleanMain

——net                          //集群通信以及客户端通信

——pool                        //tcp连接池

——raft                         //底层raft协议

——server                    //服务端

——snapshot              //数据库快照，清理WAL日志并存储已提交的数据库数据

——storage                 //底层数据库bbolt

——types                     //结构体

——utils                       //工具

——wal                        //wal机制



# Quick？Start

```
maxraftstate: 1024
snapDir:         //默认在 main/snapshot
walDir:          //默认在 main/wal
dbDir:           //默认在 main/db
servers:
  manage-server:   //暂时没用
   name: "manage"
   host: "xxxxx"
   is_local: false
  server1:
   name: "test1"
   host: "192.168.179.190"
   is_local: false   //对应节点的is_local设置为true （待改进）
  server2:
   name: "test2"
   host: "192.168.179.140"
   is_local: false
  server3:
   name: "test3"
   host: "192.168.179.95"
   is_local: true
```

服务端：

Go Run Server_main.go

init

server.yaml

若为集群变更：

Go Run Server_main.go

change

server.yaml

nodes.yaml

客户端：

Go Run Client_main.go

# 性能测试

## v1：

Key： 

5~10 Byte
Value：

7~12 Byte

![image-20250312114722515](C:\Users\takatsuki\AppData\Roaming\Typora\typora-user-images\image-20250312114722515.png)

##### 测试环境：

服务器：

三台ubuntu虚拟机
ubuntu22.04
内存2G + 硬盘SSD 20GB + 2核CPU

客户端：

一台ubuntu虚拟机
ubuntu22.04
内存2G + 硬盘SSD 20GB + 2核CPU

### Bench-Put：

QPS:50+  

![e762b61177b18847b954143d6e9b1c47](C:\Users\takatsuki\Documents\Tencent Files\2500776463\nt_qq\nt_data\Pic\2025-03\Ori\e762b61177b18847b954143d6e9b1c47.png)

### Bench-Get:

QPS:50+ 

![bfb87e42291fc09e3eef15935b35cea2](C:\Users\takatsuki\Documents\Tencent Files\2500776463\nt_qq\nt_data\Pic\2025-03\Ori\bfb87e42291fc09e3eef15935b35cea2.png)

### Bench-Concurrent(读写混合并发):

QPS: 90+

![c363c580b28bb3f2cccbae68cbf0a8f0](C:\Users\takatsuki\Documents\Tencent Files\2500776463\nt_qq\nt_data\Pic\2025-03\Ori\c363c580b28bb3f2cccbae68cbf0a8f0.png)



### 总结：

QPS太低了，经过排查调试，发现客户端单次请求的耗时在10~20ms之间，而服务器在处理整个请求的耗时在5~10ms之间。并且服务器在处理时的耗时中，耗时大头在通过tcp进行rpc通信的时候，单次rpc通信耗时在2~5ms之间。
项目中的通信统一使用encoder := gob.NewEncoder(conn)，使用gob包进行序列化并发送数据。
查询资料发现gob的性能比较差，考虑优化通信模块
![image-20250312115204757](C:\Users\takatsuki\AppData\Roaming\Typora\typora-user-images\image-20250312115204757.png)

![e556b4ac593f6ecd5918c823f8aa6eba](C:\Users\takatsuki\Documents\Tencent Files\2500776463\nt_qq\nt_data\Pic\2025-03\Ori\e556b4ac593f6ecd5918c823f8aa6eba.png)
![f0700462d6dd0577738604c96908e772](C:\Users\takatsuki\Documents\Tencent Files\2500776463\nt_qq\nt_data\Pic\2025-03\Ori\f0700462d6dd0577738604c96908e772.png)



# 项目记录：

## 整体架构：

![image-20250312155811454](C:\Users\takatsuki\AppData\Roaming\Typora\typora-user-images\image-20250312155811454.png)

## 操作流程图：

![image-20250312162757830](C:\Users\takatsuki\AppData\Roaming\Typora\typora-user-images\image-20250312162757830.png)

## 模块详解：

### 1.通信模块：

 Server在启动时会通过反射将Server具有的方法保存起来，如Raft模块的Leader Vote，Append Entries方法，服务模块的Put，Get方法。

 节点之间的连接基于TCP连接池，Server在启动时会通过配置文件创建与其他Server的tcp连接并保存为ClientEnd，Client在启动时也会通过配置文件与Server创建TCP连接保存为ClientEnd

发送请求时通过Gob Coder将参数序列化后通过conn发送，然后根据参数中的方法名进行解包调用。

### 2.底层数据库模块：

参考Etcd，基于Bbolt进行封装。

**BackEnd**：对外暴露BatchTx写事务接口以及ReadTx读事务接口。管理整个数据库

**Batch_tx**：写事务内置写缓存buf（txWriteBuffer)，可用操作为Put，Range(查询)，CreateBucket(新建桶)，DeleteBucket(删除桶),Delete(删除键)。操作时必须持有锁
只有在Put操作时候，不仅会往桶中加入kv，还会向buf中加入kv，此时事务不会提交。

在解锁时候，如果当前的未提交事务个数>0，那么会将当前写事务的buf写回到读事物的buf中，然后将写事务的buf清空。

只有当解锁时，有删除的事务或者未提交事务个数到达阈值，才会进行提交。

在写事务提交时，必须对读缓存进行清空，并保证当前没有读事务，以免产生一致性问题。
（比如，删除一个key之后，立马进行读，如果写事务提交时没有保证读缓存为空，则会造成脏读）

**Read_tx**：读事务内置读缓存buf (txReadBuffer)，支持range操作。

range优先从buf中读取数据，若读取后没满足读取需求，则会加锁查数据库

读事务向外界暴露的是ConcurrentReadTx，并发读事务是当前真正读事务的复制，只有在复制时会导致冲突，保证了并发读。

**Buffer**：buffer分为txReadBuffer和txWriteBuffer，两个buffer的底层都是bucket对应的key与value。

### 3.Wal模块：

wal的每条记录的格式为，每个wal文件设置为64mb
8byte的序列号 + 4byte的数据长度 + 4byte的CRC校验 + 实际Data

Data为raft节点的状态，重点包括raft节点的所有日志。每条日志就对应一个操作。

wal日志为批量写入文件，每条wal先写入内存，在读写事务进行提交时才会进行commit批量写入文件

wal在读取时会根据传入的seq，读取当前的所有wal文件，并取出序号seq大于传入seq的记录，并进行依次回放。

### 4.Snapshot模块：

当到达快照阈值时，会进行snapshot，保存当前数据库状态，并将当前raft节点的状态保存，然后清除之前所有的wal文件。
在崩溃恢复时，会先读取snap文件，然后根据当前的全局seq读取wal文件。

### 5.Client模块：

传递请求，等待请求。

### 6.Server模块

Server收到Client的请求后，会向raft模块传递一个操作日志，在raft完成共识之后，会Apply该操作，并将结果返回。

## 思路记录：

### 1.集群变更操作

在分布式的集群变更中，新旧配置的变更是会存在问题的：
**脑裂问题**

![raft-multi-leader.png](https://segmentfault.com/img/bVbHOw2)

部分节点还是旧配置，部分节点已经是新配置，就会出现两个集群。这两个集群分别都可以选出leader





#### Joint Consensus：

为了解决以上三个问题，引入一个过渡态

具体流程：

Client发送集群变更请求

Server收到后，Leader节点先写入一条过渡态的日志，该日志是新集群与旧集群的并集的节点配置。每个节点收到集群变更日志后立马进行应用，更改自己的通信Peer。

在过渡态日志复制到大多数节点后，进行提交，并在此时追加一条新集群的日志

新集群日志复制到每个节点，每个节点收到后检查自己是否属于新集群，不属于就shut down，属于就更改自己的通信Peer，排除旧节点。

解决问题：

只有集群配置相同且任期更新且日志最新时才会投票

1. Cold,new日志在提交之前，在这个阶段，Cold,new中的所有节点有可能处于Cold的配置下，也有可能处于Cold,new的配置下，如果这个时候原Leader宕机了，无论是发起新一轮投票的节点当前的配置是Cold还是Cold,new，都需要Cold的节点同意投票，所以不会出现两个Leader。就算是Cold的节点
2. Cold,new提交之后，Cnew下发之前，此时所有Cold,new的配置已经在Cold和Cnew的大多数节点上，如果集群中的节点超时，那么肯定只有有Cold,new配置的节点才能成为Leader，所以不会出现两个Leader
3. Cnew下发以后，Cnew提交之前，此时集群中的节点可能有三种，Cold的节点（可能一直没有收到请求）， Cold,new的节点，Cnew的节点，其中Cold的节点因为没有最新的日志的，集群中的大多数节点是不会给他投票的，剩下的持有Cnew和Cold,new的节点，无论是谁发起选举，都需要Cnew同意，那么也是不会出现两个Leader

### 2.容灾崩溃恢复：

这里设计WAL和Snapshot，通过分析得到以下四个场景

暂定机制：两阶段写入，先写入缓冲区中，事务提交时再刷盘。wal只写入raft节点状态变更的日志，不写入操作日志。 snapshot写入数据库数据以及raft状态。

还没触发snapshot的崩溃恢复：
1.leader 在收到操作日志，在写入raft日志之前崩溃。无影响，客户端超时重新请求
2.leader 收到操作日志，写入raft日志后，写入wal之前崩溃，无影响，客户端超时重新请求

3.leader收到操作日志，写入raft日志后，写入wal之后，写入到大多数节点之前崩溃。没有日志的raft节点成为leader，客户端超时重新请求，有日志的raft节点会被新leader覆盖。
而旧leader通过wal恢复，如果落后日志过多，会被发snapshot，覆盖数据库以及raft状态，不受影响。如果落后日志不多，恢复后成为follower，会被新leader进行截断日志，之后会自己应用前面的日志

幂等性保证：通过server的historyMap，当客户端选择新的leader进行请求时候，如果此时新leader已经应用了这条日志，那么historyMap中就会有缓存，就会直接返回，如果这时还没应用，在raft新增一条日志时，会对比最后一条日志和当前日志的commandIndex和ClientId,若相同则返回

4.leader收到操作日志，写入raft日志，写入wal后，写入到大多数节点之后崩溃。有新日志的raft节点成为leader。
新leader如何应用这条日志的？新leader会发送心跳，心跳被同意后，因为新leader会初始化所有matchindex为上次快照的位置，而所有nextindex默认为新leader的最后一条日志。
然后新leader会进行回退，找到之前作为follower最后提交的日志。（此时因为之前作为follower，是要收到leader的第二次日志复制请求才会更新commitindex，所以这时候作为新leader的commitIndex一定是在没应用的日志之前的）
然后进行应用，就会把之前的日志应用了。而旧leader崩溃恢复后时，如果落后日志过多，会被发snapshot，不受影响，如果落后日志不多，恢复后成为follower，leader没有这条日志，会被发送这条日志

4.follower收到日志，写入wal之前崩溃，利用wal恢复，无影响，跟随leader
5.follower收到日志，写入wal之后崩溃，利用wal恢复，无影响，收到leader心跳后，会应用之前的日志

以上只利用wal进行恢复，不会对底层数据库进行回放，因为只恢复raft的日志状态，会跟着新leader的指引应用前面的所有日志。

如果只是重启的操作，利用wal恢复raft状态，底层数据库文件还存在，并且lastapplied这些状态都存在，不会重复应用

触发了snapshot的崩溃恢复：

场景与上面相同。只是恢复的时候使用了snapshot，将底层数据库的数据进行了恢复，并且利用snapshot+wal对raft状态进行了恢复。随后依然跟随新leader将之前没应用的日志进行了应用，更新了数据库。

# TODO

1.在断言Op结构体时，有时候断言*types.Op错误，有时候断言*types.Op错误。只有if else两种类型才可

2.目前依靠配置文件启动和调试都比较麻烦，考虑引入一个中心端，实现节点发现等

3.性能方面，当前性能较差，考虑更换序列化方式为ProtoBuf

