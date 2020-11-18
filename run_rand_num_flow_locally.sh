#windowing does not working
mvn package exec:java \
-Dexec.mainClass=com.blowder.beam.ProcessRandNumsToBigQuery \
-Dexec.args="--runner=DirectRunner \
--topicName=projects/boxwood-sector-246122/topics/flow_rand_numbers_test" \
-Pdirect-runner