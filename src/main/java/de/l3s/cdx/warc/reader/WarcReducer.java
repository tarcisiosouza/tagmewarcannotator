package de.l3s.cdx.warc.reader;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WarcReducer extends Reducer<LongWritable, Text, NullWritable, Text>
{
	private final static NullWritable outKey = NullWritable.get();

public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException
{

	for (Text val : values) {
		context.write(outKey, new Text (val.toString()));
    }

}
}