mvn package exec:java \
-Dexec.mainClass=com.blowder.beam.WordCount \
-Dexec.args="--runner=DirectRunner --inputFile=gs://apache-beam-samples/shakespeare/kinglear.txt --output=${PWD}/target/word-count.txt " \
-Pdirect-runner