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

public class ColumnBasedPartitioningAtMapper extends Configured implements Tool{

    public  static class MultipleFileOnColumn extends Mapper<LongWritable, Text, NullWritable, Text>{

        private MultipleOutputs<NullWritable, Text> mos;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs<NullWritable, Text>(context);
        }

        public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedIOException, InterruptedException{
            String[] arr = value.toString().split(",", -1);
            String chrono = arr[0] + "," + arr[1] + "," + arr[2];
            String geo    = arr[0] + "," + arr[4] + "," + arr[5];
            mos.write("chrono" , NullWritable.get(), new Text(chrono));
            mos.write("geo" , NullWritable.get() , new Text(geo));

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }

    }
    public int run(String[] strings) throws Exception {

        Job job = Job.getInstance();

        job.setJobName("ColumnBasedPartitioningAtMapper name");
        job.setJarByClass(ColumnBasedPartitioningAtMapper.class);
        job.setMapperClass(MultipleFileOnColumn.class);
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
        MultipleOutputs.addNamedOutput(job,"chrono",TextOutputFormat.class,NullWritable.class, Text.class );
        MultipleOutputs.addNamedOutput(job,"geo",TextOutputFormat.class,NullWritable.class, Text.class );
        return job.waitForCompletion(true) ?0 :1;


    }

    public static void main(String []args) throws Exception{

        int rc = ToolRunner.run(new Configuration() , new ColumnBasedPartitioningAtMapper(), args);
        System.exit(rc);
    }

}

