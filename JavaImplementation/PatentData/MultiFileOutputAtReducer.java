import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
 *   which will contain record for that country.
 *   Here partitioning of record is done at Reducer.
 *   Input to this program is apat63_99.txt .
 *   Command  for running this program is :
 *   hadoop  jar  <jar-file-name> AverageCountryClaim    <HDFS path name for apat63_99.txt>  <HDFS path name for output directory>
 **/

/**
 * Created by bgautam on 9/8/2016.
 */
public class MultiFileOutputAtReducer extends Configured implements Tool {

    public  static class IdentityMapper extends Mapper<LongWritable, Text, LongWritable, Text>{

       public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedIOException, InterruptedException{
       context.write(key, value);
        }
    }

    public  static class ReduceByCountry extends Reducer<LongWritable, Text, NullWritable, Text> {

        private MultipleOutputs<NullWritable, Text> multipleOutputs;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

   public void reduce(LongWritable key, Iterable<Text> values , Context context) throws IOException,InterruptedException{
            for (Text value :values){
                String arr[] = value.toString().split(",", -1);
                String  country = arr[4].substring(1,3);
                String fileName = country +"/"+"part" ;
                multipleOutputs.write(NullWritable.get(), value, fileName);

            }
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }

    }

    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();

        job.setJobName("MultiFileOutputAtReducer name");
        job.setJarByClass(MultiFileOutputAtReducer.class);
        job.setMapperClass(IdentityMapper.class);
        job.setReducerClass(ReduceByCountry.class);

        Path in = new Path(strings[0]);
        Path out= new Path(strings[1]);
        FileSystem fs = FileSystem.get(job.getConfiguration());
        if ( fs.exists(out)){
            fs.delete(out,true);
        }
        FileInputFormat.addInputPath(job, in);
        FileOutputFormat.setOutputPath(job,out);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        return job.waitForCompletion(true) ?0 :1;


    }

    public static void main(String []args) throws Exception{

        int rc = ToolRunner.run(new Configuration() , new MultiFileOutputAtReducer(), args);
        System.exit(rc);
    }
}


