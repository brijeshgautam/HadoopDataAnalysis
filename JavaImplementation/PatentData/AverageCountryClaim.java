import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** This program computes average claim per country. 
*  Input to this program is apat63_99.txt . 
* Command  for running this program is : 
*  hadoop  jar  <jar-file-name> AverageCountryClaim    <HDFS path name for apat63_99.txt>  <HDFS path name for output directory>
**/ 


/**
 *  * Created by bgautam on 8/26/2016.
 *   */
class ClaimCount implements Writable{
    private  Text  numClaim ;
    private Text count ;
    public void readFields(DataInput in) throws IOException{
          numClaim =new Text(in.readUTF());
          count = new Text(in.readUTF());
    }
    public void write(DataOutput out) throws  IOException{
        out.writeUTF(numClaim.toString());
        out.writeUTF(count.toString());
    }
    public void set(String numClaim, Integer count){
     this.numClaim = new Text(numClaim);
        this.count = new Text(count.toString());

    }
    public Text getNumClaim(){
         return numClaim;
    }
    public Text getCount(){
        return count;
    }
}


class  MapClass extends Mapper<LongWritable , Text,Text, ClaimCount> {

    public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
        String fields[] = line.toString().split(",", -20);
        String country = fields[4];
        String numClaims = fields[8];
        if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {
            ClaimCount outObj = new ClaimCount();
            outObj.set(numClaims, 1);
            context.write(new Text(country), outObj);
        }
    }
}

class ReduceClass extends Reducer<Text, ClaimCount,Text, DoubleWritable>{
     public void reduce(Text key, Iterable<ClaimCount >values, Context context) throws IOException, InterruptedException{
         double  sum = 0;
         int count = 0;
         for ( ClaimCount obj : values ){
             sum += Double.parseDouble(obj.getNumClaim().toString());
             count += Double.parseDouble(obj.getCount().toString());
         }
         if ( count != 0)
         context.write(key, new DoubleWritable(sum/count));
     }

}

class CombineClass extends Reducer<Text, ClaimCount,Text, ClaimCount>{
    public void reduce(Text key, Iterable<ClaimCount >values, Context context) throws IOException, InterruptedException{
         Integer sum = 0;
        int count = 0;
        for ( ClaimCount obj : values ){
            sum += Integer.parseInt(obj.getNumClaim().toString());
            count += Integer.parseInt(obj.getCount().toString());
        }
        ClaimCount obj = new ClaimCount();
        obj.set(sum.toString() ,count);
        context.write(key, obj);
    }

}


public class AverageCountryClaim extends Configured implements Tool {

    public int run(String [] args) throws IOException, InterruptedException, ClassNotFoundException{
        Configuration  conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName("Average CountryClaim");
        job.setJarByClass(AverageCountryClaim.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(CombineClass.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ClaimCount.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        Path outPath = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outPath)){
            fs.delete(outPath, true);
        }
        System.exit(job.waitForCompletion(true)? 0 :1);
        return 0;
    }

    public static  void main(String []  args) throws  Exception{
        int rc = ToolRunner.run(new Configuration(), new AverageCountryClaim(), args);
        System.exit(rc);
    }
}
