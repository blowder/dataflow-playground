mvn package exec:java \
-Dexec.mainClass=com.blowder.beam.BigQueryIntro \
-Dexec.args="--runner=DirectRunner \
--project=boxwood-sector-246122 \
--tempLocation=gs://dataflow_tests_blowder_com/tmp/ \
--outputFile=${PWD}/target/data.txt " \
-Pdirect-runner