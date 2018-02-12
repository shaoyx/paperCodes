#!/bin/bash
for host in `cat hosts`; do echo $host; ssh $host ps -ef | grep attempt; echo ""; done
