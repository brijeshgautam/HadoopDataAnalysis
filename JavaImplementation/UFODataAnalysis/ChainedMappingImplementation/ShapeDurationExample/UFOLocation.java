
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ANIMESH on 04-07-2016.
 */
public class UFOLocation {

    public static class MapClass extends Mapper <LongWritable, Text, Text, LongWritable>{
        private  final static LongWritable One = new LongWritable(1);
        private  final static Pattern pattern = Pattern.compile( ".*,\\s*([a-zA-Z]{2}).*");
        public void map(LongWritable key, Text value , Context context) throws IOException ,  InterruptedException{
            String line = value.toString();
            String fields [] = line.split("\t");
            String stateInfo = fields[2].trim();
            if (stateInfo.length() >0){
                Matcher match = pattern.matcher(stateInfo);
                if (match.find( )) {
                    context.write(new Text(match.group(1)), One);
                }
            }
        }
    }

    public static class LocationReducerClass extends Reducer<Text, Iterable<LongWritable> , Text, LongWritable>{

        void reducer(Text key, Iterable<LongWritable> value,  Context context ) throws IOException, InterruptedException{
           Long total = new Long(0);
           if ( key.toString().equals("YK"))
             System.out.println(key);
            for ( LongWritable val : value){
                total = total + val.get();
            }
            context.write(key, new LongWritable(total));
        }
    }
    public static void  main(String args []) throws IOException, InterruptedException, ClassNotFoundException{
        Job job = Job.getInstance();
        Configuration conf = new Configuration(false);
        ChainMapper.addMapper(job, UFORecordValidationMapper.class, LongWritable.class, Text.class, LongWritable.class, Text.class, conf);

        Configuration extractLocation = new Configuration(false);

        ChainMapper.addMapper(job, MapClass.class, LongWritable.class, Text.class, Text.class, LongWritable.class, extractLocation);

        job.setMapperClass(ChainMapper.class);
        job.setReducerClass(LocationReducerClass.class);
        job.setJarByClass(UFOLocation.class);
        job.setJobName("Location Stats");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0: 1);
    }
}
