import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import  org.apache.hadoop.filecache.DistributedCache;

public class UFOStateDurationDriver {

    public  static  void  main(String []args)throws IOException, InterruptedException, ClassNotFoundException,URISyntaxException{
	if (args.length<3){
	    System.err.println("Input  file ,output path and state-mapping are not specified");
	    System.exit(0);
	}
	// Configuration conf = getConf();
	Job  job = Job.getInstance();
	Configuration config = new Configuration(false);
        DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
	ChainMapper.addMapper(job,UFORecordValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, config);
	Configuration durationConfig = new Configuration(false);

	ChainMapper.addMapper(job, ExtractStateDurationMapper.class,LongWritable.class, Text.class,Text.class, LongWritable.class,durationConfig);

	job.setMapperClass(ChainMapper.class);
	job.setReducerClass(GetMinMaxMeanReducer.class);
	job.setJarByClass(UFOStateDurationDriver.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true)? 0 :1);
    }
}
