# kv server
1. 支持 Put，Get，Delete 和 Scan 操作；
2. kv使用BtreeMap存储结构；
3. server和client通信使用grpcio。
4. 性质：
   - 持久性：单纯写log，recover时读log文件;
   - 并发：使用读写锁RwLock实现，写log采用batch实现，默认为1(1条kv即flush)。
## bin
- server ：kv server的服务器端；
- client ：客户端测试简单的功能；
- client_mt ：客户端多线程测试服务器；

## 遇到的问题
当client_mt的线程数目过多时，例如1000，ubuntu下最大文件打开数为1024，运行多线程客户端时会出现如下错误：
```
E0407 16:06:11.233956634    8741 ev_epollex_linux.cc:1495]   pollset_set_add_pollset: {"created":"@1554624371.233910609","description":"OS Error","errno":24,"file":"/root/.cargo/registry/src/github.com-1ecc6299db9ec823/grpcio-sys-0.4.4/grpc/src/core/lib/iomgr/ev_epollex_linux.cc","file_line":556,"os_error":"Too many open files","syscall":"epoll_create1"}
E0407 16:06:11.234174469    5065 ev_epollex_linux.cc:1445]   assertion failed: i != pss->pollset_count
Aborted (core dumped)

```
这是因为grpcio的grpcio-sys-0.4.4依赖出现打开文件数目过多错误，推测是建立grpc连接时会有打开文件操作，1000个线程建立1000个grpc导致文件打开数目过多。

解决(ubuntu 下)：
```
> ulimit -a  # 查看限制的文件数目
> ulimit -n 4096  # 将文件打开数目设为4096
```
## 多线程测试结果
```
use:14226ns , 14.23s
Thread num:1000 , every thread put:20
Key size:16 bytes , Value size:4096 bytes
# batch=1，1000个线程，每个线程put 20条key-value，用时14.23s，每秒的并发 > 1000。
```
```
use:13531ns , 13.53s
Thread num:1000 , every thread put:20
Key size:16 bytes , Value size:4096 bytes
# batch=100，1000个线程，每个线程put 20条key-value，用时13.53s，每秒的并发 > 1000。
```
batch作用不大？
这是因为写log的batch采用BufWriter写入文件，默认大小8KB，并不是整合到一起，然后写入文件，效果提升不明显，但为了保证可靠性，batch=1是常常设置的。