package com.blowder.beam;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.JavaBeanSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.lang3.math.NumberUtils;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BigQueryIntro {
    final static DateTimeFormatter BQ_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final SimpleFunction<KV<String, Double>, String> toCsvRow = new SimpleFunction<KV<String, Double>, String>() {
        @Override
        public String apply(KV<String, Double> input) {
            return input.getKey() + ";" + input.getValue();
        }
    };

    private static final SimpleFunction<MyData, KV<String, Integer>> toMeanByRegionCode = new SimpleFunction<MyData, KV<String, Integer>>() {
        @Override
        public KV<String, Integer> apply(MyData input) {
            return KV.of(input.getLocation(), NumberUtils.toInt(input.getConfirmed(), 0));
        }
    };
    private static final SimpleFunction<MyData, KV<String, String>> toRegionNameByRegionCode = new SimpleFunction<MyData, KV<String, String>>() {
        @Override
        public KV<String, String> apply(MyData input) {
            return KV.of(input.getLocation(), input.getSubregionName() == null ? "" : input.getSubregionName());
        }
    };

    public interface MyOptions extends PipelineOptions {
        @Description("Path of the file to write to")
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {
        MyOptions opts = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        final String query = String.format("SELECT * FROM bigquery-public-data.covid19_open_data_eu.covid19_open_data where " +
                        "country_code = '%s' and date > '%s' ",
                "UA",
                BQ_DATE_FORMATTER.format(LocalDateTime.now().minus(Duration.ofDays(30))));

        PCollection<MyData> myDataCollection = pipeline
                .apply(BigQueryIO.readTableRows().fromQuery(query).usingStandardSql())
                .apply(MapElements.into(TypeDescriptor.of(MyData.class)).via(MyData::fromTableRow));

        PCollection<KV<String, String>> regionCodeToName = myDataCollection.apply(MapElements.via(toRegionNameByRegionCode))
                .apply(Distinct.create());
        PCollection<KV<String, Double>> meanToName = myDataCollection.apply(MapElements.via(toMeanByRegionCode))
                .apply(Mean.perKey());

        TupleTag<String> regionCodeTag = new TupleTag<>();
        TupleTag<Double> meanTag = new TupleTag<>();

        KeyedPCollectionTuple.of(regionCodeTag, regionCodeToName)
                .and(meanTag, meanToName)
                .apply(CoGroupByKey.create())
                .apply(ParDo.of(
                        new DoFn<KV<String, CoGbkResult>, KV<String, Double>>() {
                            @ProcessElement
                            public void processElement(ProcessContext c) {
                                KV<String, CoGbkResult> e = c.element();
                                for (String regionName : e.getValue().getAll(regionCodeTag)) {
                                    for (Double mean : e.getValue().getAll(meanTag)) {
                                        c.output(KV.of(e.getKey() + ":(" + regionName + ")", mean));
                                    }
                                }
                            }
                        }))
                .apply("MyData to csv row", MapElements.via(toCsvRow))
                .apply("WriteData", TextIO.write().to(opts.getOutputFile()));
        pipeline.run().waitUntilFinish();

    }

    @DefaultSchema(JavaBeanSchema.class)
    static class MyData {
        private String date;
        @Nullable
        private String confirmed;
        private String location;
        @Nullable
        private String subregionName;

        @SchemaCreate
        public MyData(String date, @Nullable String confirmed, String location, @Nullable String subregionName) {
            this.date = date;
            this.confirmed = confirmed;
            this.location = location;
            this.subregionName = subregionName;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        @Nullable
        public String getConfirmed() {
            return confirmed;
        }

        public void setConfirmed(@Nullable String confirmed) {
            this.confirmed = confirmed;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        @Nullable
        public String getSubregionName() {
            return subregionName;
        }

        public void setSubregionName(@Nullable String subregionName) {
            this.subregionName = subregionName;
        }

        public static MyData fromTableRow(TableRow tableRow) {
            String date = (String) tableRow.get("date");
            String confirmed = (String) tableRow.get("new_confirmed");
            String location = (String) tableRow.get("location_key");
            String subregionName = (String) tableRow.get("subregion1_name");
            return new MyData(date, confirmed, location, subregionName);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MyData myData = (MyData) o;

            if (date != null ? !date.equals(myData.date) : myData.date != null) return false;
            if (confirmed != null ? !confirmed.equals(myData.confirmed) : myData.confirmed != null) return false;
            if (location != null ? !location.equals(myData.location) : myData.location != null) return false;
            return subregionName != null ? subregionName.equals(myData.subregionName) : myData.subregionName == null;
        }

        @Override
        public int hashCode() {
            int result = date != null ? date.hashCode() : 0;
            result = 31 * result + (confirmed != null ? confirmed.hashCode() : 0);
            result = 31 * result + (location != null ? location.hashCode() : 0);
            result = 31 * result + (subregionName != null ? subregionName.hashCode() : 0);
            return result;
        }
    }
}
