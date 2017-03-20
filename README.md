# hbase_python
hbase connection tool

下载 thrift-0.10.0
<code>
tar -zxvf thrift-0.10.0.tar.gz
mv thrift-0.10.0 /usr/local/
./configure --with-boost=/usr/include/
</code>

如果出错了，可能缺少某些库
centos:
<code>
yum -y install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel Python-devel ruby-devel crypto-utils openssl openssl-devel
</code>

ubuntu:
<code>
apt-get install automake libtool flex bison pkgconfig gcc-c++ boost-devel libevent-devel zlib-devel Python-devel ruby-devel crypto-utils openssl openssl-devel
</code>

生产 thrift
<code>
make
make install
thrift
</code>
如果输入thrift没有错误，证明安装成功

下载 hbase-1.1.2-src.tar.gz
进入这个目录 hbase-1.1.2/hbase-thrift/src/main/resources/org/apache/hadoop/hbase/
<code>
thrift --gen py Hbase.thrift
</code>

得到我们需要的hbase python库，复制gen-py文件 到python libs ->site-packages
最后，启动hbase， 以及hbase的thrift服务
<code>
hbase-daemon.sh start thrift
</code>

<code>
jps
13447 ThriftServer
17627 HMaster
18827 Jps
22860 HRegionServer
</code>
如果看到 ThriftServer， HMaster， 就代表成功启动服务。


