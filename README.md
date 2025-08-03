## Mako: Speculative Distributed Transactions with Geo-Replication (OSDI'25) 

NOTE: This is the archive of the Mako prototype evaluated in the OSDI '25 paper. For its follow-up development, refer to this [repo](https://github.com/makodb/mako).

First, please prepare a local server (e.g., your own laptop), and this server can access to all Azure servers. Let's assume the server name is "zoo-002". All Azure servers are managed via NFS.

On zoo-002
```
git clone https://github.com/stonysystems/mako.git
cd ~/mako
# Update ips.pub, ips, and n_partitions, 
# Update and `master_pub`, `passwd`, and `master` (nfs master) in ./bash/batch_op_zoo2.sh

cd ./bash
bash batch_op_zoo2.sh conn
bash batch_op_zoo2.sh async_op "sudo apt update"
bash batch_op_zoo2.sh async_op "sudo apt-get --assume-yes install git nfs-kernel-server nfs-common"
```

On NFS-master
```
/etc/exports
/home/rolis/eRPC *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/D2PC *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/mako*(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/rdma-core *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/dpdk-stable-19.11.5 *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/janus-tapir *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/logs *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/calvin *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/janus *(rw,sync,no_subtree_check,no_root_squash)
/home/rolis/depfast-ae *(rw,sync,no_subtree_check,no_root_squash)
mkdir -p eRPC mako rdma-core dpdk-stable-19.11.5 D2PC logs janus-tapir calvin janus depfast-ae
sudo systemctl restart nfs-kernel-server
sudo systemctl status nfs-kernel-server
```

On zoo-002
```
bash batch_op_zoo2.sh cluster_nfs
```

On NFS master
```
git clone https://github.com/stonysystems/mako.git
```

On zoo-002
```
bash batch_op_zoo2.sh install20.04
```

On NFS master
```
cd ~/mako && bash install_erpc.sh
```

On zoo-002
```
bash batch_op_zoo2.sh async_op "cd ~/rdma-core && sudo make install -j10"
bash batch_op_zoo2.sh op "cd ~/dpdk-stable-19.11.5 && sudo make install T=x86_64-native-linuxapp-gcc DESTDIR=/usr -j32"
bash batch_op_zoo2.sh async_op "sudo add-apt-repository ppa:canonical-server/server-backports -y"

bash batch_op_zoo2.sh async_op "sudo sysctl -w vm.nr_hugepages=4096"2
bash batch_op_zoo2.sh op "sudo sysctl -w kernel.shmmni=8192"

bash batch_op_zoo2.sh async_op "sudo apt-get install -y dpdk"
bash batch_op_zoo2.sh init
bash batch_op_zoo2.sh async_op "bash ~/mako/installmem.sh"
bash batch_op_zoo2.sh op "sudo dpdk-devbind --status | head -7"
bash batch_op_zoo2.sh async_op "bash ~/mako/initial.sh"
# eth2 could be the first one
bash batch_op_zoo2.sh op "sudo dpdk-devbind --status | head -7"
bash batch_op_zoo2.sh op "ls -lh ~/mako| wc -l"
bash batch_op_zoo2.sh op "lsof -i:6001"
bash batch_op_zoo2.sh async_op "sudo ldconfig"
bash batch_op_zoo2.sh async_op '[ -f ~/.ssh/id_rsa ] && echo "Key file ~/.ssh/id_rsa already exists. Not overwriting." || ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa'
```

On NFS-master
```
cd $HOME/mako

# Update ips, ips.pub, n_partitions (last line leaves an enter!)

# Generate configurations
cd $HOME/mako/bash
python3 convert_ip.py

cd $HOME/mako/config
python generator.py

cd $HOME/mako/third-party/paxos/config/1leader_2followers
python generator.py

cd $HOME/mako/
make paxos

cd masstree; ./bootstrap.sh; ./configure  CC="cc" CXX="g++" --enable-max-key-len=1024 --disable-assertions --disable-invariants --disable-preconditions --with-malloc=jemalloc

cd ~/mako
make clean
# A test compile
MODE=perf make -j32 dbtest PAXOS_LIB_ENABLED=1 DISABLE_MULTI_VERSION=0 \
                           MICRO_BENCHMARK=1 TRACKING_LATENCY=0 SHARDS=1 \
                           MERGE_KEYS_GROUPS=1 SPANNER=0 MEGA_BENCHMARK=0 \
                           FAIL_NEW_VERSION=1 TRACKING_ROLLBACK=0
```

On zoo-002
```
bash batch_op_zoo2.sh async_op "bash ~/mako/kill.sh"

# Update ips for leaders/learners/followers and passwd in batch-runner.sh
cd ~/mako/bash
bash ../batch-runner.sh 1 1
```
