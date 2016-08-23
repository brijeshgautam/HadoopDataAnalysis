import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.*;
import  org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Iterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Counter;

/**
 *  * Created by bgautam on 8/18/2016.
 *   */


public class InvertPatentRecord {
 public     enum LogCounter {TOTAL_RECORD, TOTAL_REDUCE };

public  static class MapPatentRecord extends Mapper<Text, Text, Text, Text> {

    public void map(Text key , Text value, Context context) throws IOException, InterruptedException
    {
           context.write(value, key);
           Counter counter = context.getCounter(LogCounter.class.getName(), LogCounter.TOTAL_RECORD.toString());
	  counter.increment(1);
    }
 }

public static class ReducePatentRecord extends Reducer < Text, Text, Text, Text > {

    public void reduce (Text key, Iterable<Text>values , Context context) throws IOException, InterruptedException
    {
        String csv = "";
        Iterator<Text> itr = values.iterator();
        while (itr.hasNext()){
            if (csv.length() > 0){
                csv += ",";
            }
            csv += itr.next().toString();
        }
        Counter counter = context.getCounter(LogCounter.class.getName(), LogCounter.TOTAL_REDUCE.toString());
        counter.increment(1);
        context.write(key, new Text(csv));
    }
}
      public static  void main(String [] args) throws  IOException, InterruptedException, ClassNotFoundException{
        Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        Job job = Job.getInstance(conf, "invertpatentrecord");
        job.setJarByClass(InvertPatentRecord.class);
        job.setMapperClass(MapPatentRecord.class);
        job.setReducerClass(ReducePatentRecord.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        Path  outputPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputPath);
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath)){
               fs.delete(outputPath,true);
        }
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setJobName("Invert Patent Record");
        boolean rc =job.waitForCompletion(true);
        System.exit(rc == true ? 0 : 1);
    }
}
