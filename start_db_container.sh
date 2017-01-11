#!/bin/bash

while getopts ":i:" opt; do
	case $opt in
		i)
			imageName=$OPTARG
			echo "imageName = $imageName"
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
			echo "Starting docker container with imageName=$imageName and name=akka-postgres-db"
			docker run -d --name=akka-postgres-db -l deployer=akkadotnet -e POSTGRES_PASSWORD=postgres $imageName
			;;
		:)
			echo "imageName (-i) argument required" >&2
			exit 1
			;;
		\?)
			echo "imageName (-i) flag is required" >&2
			exit 1
			;;
	esac
done