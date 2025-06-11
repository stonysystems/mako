dpdk_version="dpdk-stable-19.11.5"

read_uniq_host() {
    local file="$1"
    local __resultvar="$2"

    if [[ ! -f "$file" ]]; then
        echo "Error: File '$file' not found." >&2
        return 1
    fi

    declare -A ip_map
    while IFS= read -r ip; do
        [[ -z "$ip" ]] && continue
        ip_map["$ip"]=1
    done < "$file"

    local result=("${!ip_map[@]}")
    eval "$__resultvar=(\"\${result[@]}\")"
}

master_pub="128.24.96.2"
# In ips.pub and ips: 4-fold, the order is [learner] -> [localhost] -> [p1] -> [p2]
read_uniq_host "ips.pub" hosts_pub
master="172.19.0.20"
read_uniq_host "ips" hosts 

echo "Total unique hosts_pub: ${#hosts_pub[@]}, hosts: ${#hosts[@]}"

cmd=$1
echo "cmd: $cmd"
passwd="password"

if [ $cmd == 'conn' ]; then
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        echo "ssh to reqest to $host"
        cmd="echo 'connected'"
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host $cmd
    done
elif [ $cmd == 'only_master' ]; then 
    echo "Execute on master with cmd: $2"
    sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$master_pub "$2"

elif [ $cmd == 'sshscan' ]; then
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        ssh-keyscan $host
    done
elif [ $cmd == 'async_op' ]; then
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        echo "op: $host, $2"
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "$2" &
    done

elif [ $cmd == 'master_nfs' ]; then
    echo "Permission denied; do it $master_pub"

elif [ $cmd == 'install22.04' ]; then
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "bash ~/mako/install22.04.sh" &
    done

elif [ $cmd == 'install20.04' ]; then
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "bash ~/mako/install.sh" &
    done

elif [ $cmd == 'op' ]; then
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        echo "op: $host, $2"
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "$2"

    done

elif [ $cmd == 'install-janus' ]; then
op=$(cat <<-EOF
echo 'curl https://bootstrap.pypa.io/pip/2.7/get-pip.py --output get-pip.py
sudo apt install python3-pip
sudo python2 get-pip.py' > x.sh
chmod +x x.sh
bash x.sh
pip2 install pyyaml 
pip2 install tabulate
pip3 install simplerpc
EOF
)
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        echo "op: $host, $op"
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "$op" &
    done


elif [ $cmd == 'init' ]; then
op=$(cat <<-EOF
echo 'sudo modprobe ib_uverbs
sudo modprobe mlx5_ib
sudo modprobe mlx4_ib
sudo bash -c "echo 2048 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
sudo mkdir -p /mnt/huge
sudo mount -t hugetlbfs nodev /mnt/huge' > init.sh
chmod +x init.sh
bash init.sh
EOF
)
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        echo "op: $host, $op"
        sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "$op" &
    done

elif [ $cmd == 'cluster_nfs' ]; then
op=$(cat <<-EOF
echo 'export nfs_master="$master"
mkdir -p eRPC mako rdma-core $dpdk_version logs janus-tapir calvin janus depfast-ae D2PC
sudo umount $HOME/eRPC
sudo umount $HOME/mako
sudo umount $HOME/D2PC
sudo umount $HOME/rdma-core
sudo umount $HOME/janus-tapir
sudo umount $HOME/calvin
sudo umount $HOME/$dpdk_version
sudo umount $HOME/logs
sudo umount $HOME/janus
sudo umount $HOME/depfast-ae
sudo mount -t nfs -vvvv $master:$HOME/eRPC $HOME/eRPC
sudo mount -t nfs -vvvv $master:$HOME/D2PC $HOME/D2PC
sudo mount -t nfs -vvvv $master:$HOME/mako $HOME/mako
sudo mount -t nfs -vvvv $master:$HOME/rdma-core $HOME/rdma-core
sudo mount -t nfs -vvvv $master:$HOME/janus-tapir $HOME/janus-tapir
sudo mount -t nfs -vvvv $master:$HOME/calvin $HOME/calvin
sudo mount -t nfs -vvvv $master:$HOME/janus $HOME/janus
sudo mount -t nfs -vvvv $master:$HOME/depfast-ae $HOME/depfast-ae
sudo mount -t nfs -vvvv $master:$HOME/$dpdk_version $HOME/$dpdk_version
sudo mount -t nfs -vvvv $master:$HOME/logs $HOME/logs' > nfs.sh
chmod +x nfs.sh
bash nfs.sh
ls ~/logs/
EOF
)
    for i in "${!hosts_pub[@]}"
    do
        host=${hosts_pub[$i]}
        if [ $host == $master_pub ]; then
            # do nothing
            echo "Permission denied; do it $master_pub"
        else
            echo "host: $host"
            sshpass -p$passwd ssh -o StrictHostKeyChecking=no rolis@$host "$op" &
        fi
    done
else
	:
fi 


echo "Wait for jobs..."
FAIL=0
for job in `jobs -p`
do
    wait $job || let "FAIL+=1"
done

if [ "$FAIL" == "0" ];
then
    echo "YAY!"
else
    echo "FAIL! ($FAIL)"
fi
