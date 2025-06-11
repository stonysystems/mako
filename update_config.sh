cd $HOME/mako

# Update ips, ips.pub, n_partitions (last line leaves an enter!)

# Generate configurations
cd $HOME/mako/bash
python3 convert_ip.py

cd $HOME/mako/config
python generator.py

cd $HOME/mako/third-party/paxos/config/1leader_2followers
python generator.py

cd $HOME/mako
