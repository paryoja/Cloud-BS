#!/bin/bash

output="hdfs:///user/ubuntu/chr1"
indexDir=$output"/index/*"
hdfs dfs -mv $indexDir $output
