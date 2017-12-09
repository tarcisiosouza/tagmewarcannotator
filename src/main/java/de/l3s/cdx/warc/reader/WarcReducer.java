package de.l3s.cdx.warc.reader;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class WarcReducer extends Reducer<LongWritable, Text, NullWritable, Text>
{
	private final static NullWritable outKey = NullWritable.get();

public void reduce(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
		throws IOException, InterruptedException, URISyntaxException {

		context.write(outKey, new Text (value.toString()));
}
}