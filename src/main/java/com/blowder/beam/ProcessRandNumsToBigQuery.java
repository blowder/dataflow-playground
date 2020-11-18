package com.blowder.beam;

import com.blowder.model.NumberWrapperOuterClass;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.io.Resources;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;

public class ProcessRandNumsToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(WordCount.class);

    public interface Options extends PipelineOptions, StreamingOptions, GcpOptions {

        @Validation.Required
        String getTopicName();

        void setTopicName(String topicName);

    }

    public static void main(String[] args) {
        // Begin constructing a pipeline configured by commandline flags.
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(PubsubIO.readProtos(NumberWrapperOuterClass.NumberWrapper.class)
                .fromTopic(options.getTopicName()))
                .apply(ParDo.of(new DoFn<NumberWrapperOuterClass.NumberWrapper, NumberWrapperOuterClass.NumberWrapper>() {
                    @ProcessElement
                    public void processElement(@Element NumberWrapperOuterClass.NumberWrapper element, OutputReceiver<NumberWrapperOuterClass.NumberWrapper> out) {
                        out.outputWithTimestamp(element, Instant.now());
                    }
                }))
                .apply(Window.<NumberWrapperOuterClass.NumberWrapper>into(FixedWindows.of(Duration.standardMinutes(1)))
                        .triggering(AfterWatermark.pastEndOfWindow())
                        .withAllowedLateness(Duration.ZERO)
                        .discardingFiredPanes())
                .apply(MapElements.via(new SimpleFunction<NumberWrapperOuterClass.NumberWrapper, Integer>() {
                    @Override
                    public Integer apply(NumberWrapperOuterClass.NumberWrapper input) {
                        return input.getNumber();
                    }
                }))
                .apply(Mean.<Integer>globally().withoutDefaults())
                .apply(MapElements.via(new SimpleFunction<Double, TableRow>() {
                    @Override
                    public TableRow apply(Double input) {
                        LOG.info("Average per minute is " + input);
                        return new TableRow().set("mean", input);
                    }
                }))
                .apply(
                        BigQueryIO.writeTableRows()
                                .to(String.format("%s:%s.%s", options.getProject(), "test_dataset", "test_mean"))
                                .withJsonSchema(getSchema())
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                );
        pipeline.run().waitUntilFinish();

    }

    private static String getSchema() {
        try {
            URL url = Resources.getResource("test_mean_schema.json");
            return Resources.toString(url, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
