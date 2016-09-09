import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.io.InterruptedIOException;

/** This program partition record by country. For each country , there will be separate file created 
*  which will contain record for that country.
*  Input to this program is apat63_99.txt .
* Command  for running this program is :
*  hadoop  jar  <jar-file-name> AverageCountryClaim    <HDFS path name for apat63_99.txt>  <HDFS path name for output directory>
**/

/**
 * Created by bgautam on 9/8/2016.
 */
public class MultiFileOutput extends Configured implements Tool {

    public  static class MapMultiFile extends Mapper<LongWritable, Text, NullWritable, Text>{

        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

       public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedIOException, InterruptedException{
           String arr[] = value.toString().split(",", -1);
           String  country = arr[4].substring(1,3);
           String fileName = country +"/"+"part" ;
           multipleOutputs.write(NullWritable.get(), value, fileName);
       }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

    }
    public int run(String[] strings) throws Exception {

         Job job = Job.getInstance();

        job.setJobName("MultiFileOutput name");
        job.setJarByClass(MultiFileOutput.class);
        job.setMapperClass(MapMultiFile.class);
        job.setNumReduceTasks(0);

        Path in = new Path(strings[0]);
        Path out= new Path(strings[1]);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if ( fs.exists(out)){
            fs.delete(out,true);
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job,out);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ?0 :1;


    }

    public static void main(String []args) throws Exception{

        int rc = ToolRunner.run(new Configuration() , new MultiFileOutput(), args);
        System.exit(rc);
    }
}
