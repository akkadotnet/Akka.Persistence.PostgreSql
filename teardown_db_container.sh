#!/bin/bash

echo "Tearing down any existing containers with deployer=akkadotnet"
runningContainers=$(docker ps -aq -f label=deployer=akkadotnet)
if [ ${#runningContainers[@]} -gt 0 ]
  then
	for i in $runningContainers
		do
			if [ "$i" != "" ] # 1st query can return non-empty array with null element
				then
					docker stop $i
					docker rm $i
			fi
		done
fi