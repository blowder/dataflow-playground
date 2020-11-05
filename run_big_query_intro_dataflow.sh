mvn compile exec:java \
    -Dexec.mainClass=com.blowder.beam.BigQueryIntro \
    -Dexec.args="--project=boxwood-sector-246122 \
    --gcpTempLocation=gs://dataflow_tests_blowder_com/tmp/ \
    --tempLocation=gs://dataflow_tests_blowder_com/tmp/ \
    --outputFile=gs://dataflow_tests_blowder_com/bq_output \
    --runner=DataflowRunner \
    --jobName=dataflow-bq-intro \
    --region=us-central1" \
            -Pdataflow-runner
