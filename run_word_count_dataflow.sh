mvn compile exec:java \
    -Dexec.mainClass=com.blowder.beam.WordCount \
    -Dexec.args="--project=boxwood-sector-246122 \
    --gcpTempLocation=gs://dataflow_tests_blowder_com/tmp/ \
    --output=gs://dataflow_tests_blowder_com/output \
    --runner=DataflowRunner \
    --jobName=dataflow-intro \
    --region=us-central1" \
            -Pdataflow-runner