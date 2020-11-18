mvn compile exec:java \
    -Dexec.mainClass=com.blowder.beam.ProcessRandNumsToBigQuery \
    -Dexec.args="--project=boxwood-sector-246122 \
    --topicName=projects/boxwood-sector-246122/topics/flow_rand_numbers_test \
    --runner=DataflowRunner \
    --jobName=dataflow-intro-2 \
    --region=us-central1" \
                -Pdataflow-runner