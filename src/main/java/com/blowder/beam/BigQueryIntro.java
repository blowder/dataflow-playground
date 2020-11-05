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
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.TypeDescriptor;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BigQueryIntro {

    public interface MyOptions extends PipelineOptions {
        @Description("Path of the file to write to")
        String getOutputFile();

        void setOutputFile(String value);
    }

    public static void main(String[] args) {
        MyOptions opts = PipelineOptionsFactory.fromArgs(args).as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(opts);

        final String query = "SELECT * FROM bigquery-public-data.covid19_open_data_eu.covid19_open_data limit 10";

        final SimpleFunction<MyData, String> mapper = new SimpleFunction<MyData, String>() {
            @Override
            public String apply(MyData input) {
                return input.toCsvRow();
            }
        };

        pipeline
                .apply(
                        "Read from BigQuery query",
                        BigQueryIO.readTableRows()
                                .fromQuery(query)
                                .usingStandardSql())
                .apply(
                        "TableRows to MyData",
                        MapElements.into(TypeDescriptor.of(MyData.class)).via(MyData::fromTableRow))
                .apply("MyData to csv row", MapElements.via(mapper))
                .apply("WriteData", TextIO.write().to(opts.getOutputFile()));
        pipeline.run().waitUntilFinish();

    }

    @DefaultSchema(JavaBeanSchema.class)
    static class MyData {
        private String date;
        @Nullable
        private Integer confirmed;
        private String location;

        @SchemaCreate
        public MyData(String date, @Nullable Integer confirmed, String location) {
            this.date = date;
            this.confirmed = confirmed;
            this.location = location;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        @Nullable
        public Integer getConfirmed() {
            return confirmed;
        }

        public void setConfirmed(@Nullable Integer confirmed) {
            this.confirmed = confirmed;
        }

        public String getLocation() {
            return location;
        }

        public void setLocation(String location) {
            this.location = location;
        }

        public static MyData fromTableRow(TableRow tableRow) {
            String date = (String) tableRow.get("date");
            Integer confirmed = (Integer) tableRow.get("new_confirmed");
            String location = (String) tableRow.get("location_key");
            return new MyData(date, confirmed, location);
        }

        public String toCsvRow() {
            List<String> fieldValues = new ArrayList<>();
            for (Field field : this.getClass().getDeclaredFields()) {
                field.setAccessible(true);
                try {
                    fieldValues.add(Objects.toString(field.get(this)));
                } catch (IllegalAccessException e) {
                    fieldValues.add("");
                }
            }
            return String.join(", ", fieldValues);
        }
    }
}
