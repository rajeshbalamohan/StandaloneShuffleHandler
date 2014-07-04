Context:
Shuffle errors at 10 TB scale: https://issues.apache.org/jira/browse/TEZ-1223

Purpose of this tool, is to simulate shuffleHandler on a single node and to
use jmeter to stress test locally instantiated shuffle handler.

Installation:
=============
1. mvn clean package

2. java -cp  ./target/*:./target/lib/*: org.apache.hadoop.mapred.ShuffleHandler
 <base_dir_where_shuffle_out_needs_to_be_generated>
 <number_of_partitions_per_map_output_file>
 <number_of_keys_per_partition>
 <number_of_map_output_files>

This would generate the relevant shuffle data in the folder you specified.

3. e.g: java -cp ./target/*:./target/lib/*: org.apache.hadoop.mapred
.ShuffleHandler /grid/4/home/rajesh/shuffle/ShuffleHandler/user_cache 1000 100000 1000

4. On another terminal, download jmeter and copy ./target/lib/ to
$JMETER_HOME/lib.  Copy ./target/shuffle*.jar $JMETER_HOME/lib/
$JMETER_HOME/lib/ext

5. ./jmeter -n -t ../shuffleHandler_v1.jmx (adjust settings in
shuffleHandler_v1.jmx as required)

