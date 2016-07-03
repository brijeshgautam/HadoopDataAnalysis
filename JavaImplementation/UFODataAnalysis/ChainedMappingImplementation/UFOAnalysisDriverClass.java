import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UFOAnalysisDriverClass {

    public static void main(String[] args) throws Exception
    {

        Job job = Job.getInstance();
        Configuration config = new Configuration(false) ;
        ChainMapper.addMapper(job, UFORecordValidationMapper.class, LongWritable.class,
                Text.class, LongWritable.class , Text.class, config);

        Configuration extractTimeConf= new Configuration(false);

        ChainMapper.addMapper( job,  MapExtractTimeClass.class, LongWritable.class, Text.class,
                Text.class, LongWritable.class,  extractTimeConf) ;

        job.setMapperClass(ChainMapper.class);
        job.setJarByClass(UFOAnalysisDriverClass.class);
        job.setReducerClass(GetMinMaxMeanReducer.class);
        FileInputFormat.setInputPaths(job,new Path(args[0])) ;
        FileOutputFormat.setOutputPath(job, new Path(args[1])) ;
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
