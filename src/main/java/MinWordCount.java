import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;


public class MinWordCount {
    public static void main(String[] args) {
        // Create a PipelineOptions object. This object lets us set various execution
        // options for our pipeline, such as the runner you wish to use. This example
        // will run with the DirectRunner by default, based on the class path configured
        // in its dependencies.
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);
        p.apply(TextIO.read().from("input.txt"))
                .apply("ExtractWords", FlatMapElements
                        .into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements
                        .into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply(TextIO.write().to("output.txt"));
        p.run().waitUntilFinish();
    }
}
