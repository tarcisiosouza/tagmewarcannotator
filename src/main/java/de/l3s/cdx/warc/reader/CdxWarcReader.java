package de.l3s.cdx.warc.reader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveRecord;
import org.archive.io.arc.ARCReaderFactory;
import org.archive.io.warc.WARCReaderFactory;

public final class CdxWarcReader 
{
	
	public static class WarcMapper extends
	Mapper<Object, Text, Object, Text> {

private final static NullWritable outKey = NullWritable.get();
private int rescode;
private String WarcFilePath;
private long offset;
private long compressedsize;
private String CdxFileName;
private String type;
private String WarcFileName;
public String url;
private String[] allMatches = new String[1];
private String str;
private URL Url;
private int test = 0;
private static String filePath;

private static HashMap<String, String> domainsCategories = new HashMap<String, String>();
private static HashSet<String> domains = new HashSet<String>();
private static HashMap<String,String> filePaths = new HashMap<String,String>();

@Override
protected void setup(Context context) throws IOException,
		InterruptedException {
	
	filePath = "";

	}

public String getDomain (String url) throws MalformedURLException
{
	Matcher m = Pattern.compile("(http).*").matcher(url);
	while (m.find()) 
    {
		
		allMatches[0] = m.group(); 
    	str = allMatches[0];
    	Url = new URL(str);
    }
    
	String Domain = Url.getHost();
	if (Domain.contains("www")) {
		int index = Domain.indexOf(".");
		Domain = Domain.substring(index + 1, Domain.length());
	}
	
	return Domain;
	
}


public void map(Object key, Text value, Context context)
		throws IOException, InterruptedException {
	
	test++;
/*
	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	CdxFileName = fileSplit.getPath().getName();
	URL Url;
	
	if (CdxFileName.contains("_SUCESS") || CdxFileName.contains("_index")
			|| CdxFileName.contains("_masterindex"))
		return;

	
	StringTokenizer token = new StringTokenizer(value.toString());
	try 
	{
		token.nextToken();
		token.nextToken();
	} catch (Exception e) {
		return;

	}

	try 
	{
		url = token.nextToken();
		type = token.nextToken();
	
		
	} catch (Exception e) {
		return;
	}

	try 
	{
		token.nextToken();
		token.nextToken();
		token.nextToken();
		token.nextToken();
		token.nextToken();
		token.nextToken();
		//offset = Long.valueOf(token.nextToken()).longValue();
		WarcFileName = token.nextToken();
		
	} catch (Exception e) {
		return;
	}

	//type = WarcFileName.substring(0, 2);
	
	//WarcFilePath = "hdfs://nameservice1/data/ia/w/de/" + type + "/" + WarcFileName;
	String url = "hdfs://nameservice1/data/british_lib/";
	//String url = "/home/souza/british_lib_test";
	try {
		getFilesRecursively (url,WarcFileName,context);
	} catch (URISyntaxException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	filePath=filePath.replaceAll("file:", "");
	*/
	
	context.write(key, new Text (value.toString()));
	
}

public String getUrl() {
	return url;
}

public void setUrl(String url) {
	this.url = url;
}



public static void getFilesRecursively(String Url,String fileName,Context context) throws IOException, URISyntaxException {
	
    Path location = new Path(Url);
    
	FileSystem fileSystem = location.getFileSystem(context.getConfiguration());

	FileStatus[] status = fileSystem.listStatus(new Path(Url));
	
	for (int i = 0; i < status.length; i++) {
	    if (status[i].isDirectory()) {
	        getFilesRecursively(status[i].getPath().toString(),fileName,context);
	    } else {
	        try {
	        	
	        	//filePath = status[i].getPath().toString();
	          if  (status[i].getPath().toString().contains(fileName))
	          {
	        	  filePath = status[i].getPath().toString();
	        	  return;
	          }
	        } catch (Exception e) {
	            System.err.println(e.toString());
	        }

	    }

	}

}
public class ArcWarcFilenameFilter {
	
	public final static String ARC_SUFFIX = ".arc";
	public final static String ARC_GZ_SUFFIX = ".arc.gz";
	public final static String WARC_SUFFIX = ".warc";
	public final static String WARC_GZ_SUFFIX = ".warc.gz";
	public final static String OPEN_SUFFIX = ".open";
	
	
}
public static void ArchiveRecordIterator (String p,long offset, Context context, URL url) throws IOException, InterruptedException {
	  
	 
	  Configuration conf=context.getConfiguration();
	  FileSystem fs=FileSystem.get(conf);
	  Path path=new Path(p);
	  FSDataInputStream is=fs.open(path);
	  
	  is.seek(89653723);
	  
	  ArchiveReader reader = null;
	  
	  
	  if (isArc(path.getName())) {
	    reader=ARCReaderFactory.get(path.getName(),is,false);
	    
	  }
	 else   if (isWarc(path.getName())) {
	    reader=WARCReaderFactory.get(path.getName(),is,false); 
	  
	 }
	  
	  
	  for(ArchiveRecord record : reader)
	    {
				//if (u.contentEquals(url.toString()))
			//	if (record.getHeader().getOffset()==offset)
				context.write (outKey,new Text (record.getHeader().getUrl() + " "+ url + " " + record.getHeader().getOffset()+ " "+ offset));
			    
	    	
	    }

}
	
	private static boolean isArc(final String name) {

		return (name.endsWith(ArcWarcFilenameFilter.ARC_SUFFIX)
				|| name.endsWith(ArcWarcFilenameFilter.ARC_GZ_SUFFIX));
	}

	private static boolean isWarc(final String name) {

		return (name.endsWith(ArcWarcFilenameFilter.WARC_SUFFIX)
			|| name.endsWith(ArcWarcFilenameFilter.WARC_GZ_SUFFIX));	
	}

	
}
	public static void main(String[] args) throws IOException,
	InterruptedException, ClassNotFoundException {

Path inputPath = new Path(args[0]);
Path outputDir = new Path(args[1]);

// Create configuration
Configuration conf = new Configuration(true);
conf.setInt("yarn.nodemanager.resource.memory-mb", 58000);
conf.setInt("yarn.scheduler.minimum-allocation-mb", 3000);
conf.setInt("yarn.scheduler.maximum-allocation-mb", 58000);
conf.setInt("mapreduce.map.memory.mb", 3000);
conf.setInt("mapreduce.reduce.memory.mb", 6000);
conf.setInt("yarn.scheduler.minimum-allocation-mb", 3000);
conf.setInt("yarn.app.mapreduce.am.resource.mb", 6000);
conf.set("mapreduce.map.java.opts", "-Xmx2400m");
conf.set("mapreduce.reduce.java.opts", "-Xmx4800m");
conf.set("mapred.map.child.java.opts", "-Xmx2400m");
// -Xmx512m
conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx4800m");
// conf.setInt("mapreduce.task.io.sort.mb", 15);
conf.setInt("mapreduce.task.io.sort.mb", 1000);

Job job = Job.getInstance(conf);
job.setJarByClass(CdxWarcReader.class);

// Setup MapReduce
job.setMapperClass(WarcMapper.class);
job.setReducerClass(WarcReducer.class);
job.setNumReduceTasks(100);

// Specify key / value
job.setOutputKeyClass(LongWritable.class);
job.setOutputValueClass(Text.class);
job.setMapOutputKeyClass(LongWritable.class);
job.setMapOutputValueClass(Text.class);
// Input
FileInputFormat.addInputPath(job, inputPath);
job.setInputFormatClass(TextInputFormat.class);

// Output
FileOutputFormat.setOutputPath(job, outputDir);
job.setOutputFormatClass(TextOutputFormat.class);
LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
// Delete output if exists
FileSystem hdfs = FileSystem.get(conf);
if (hdfs.exists(outputDir))
	hdfs.delete(outputDir, true);

FileOutputFormat.setCompressOutput(job, true);
FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
// Execute job
int code = job.waitForCompletion(true) ? 0 : 1;
System.exit(code);

}
}
