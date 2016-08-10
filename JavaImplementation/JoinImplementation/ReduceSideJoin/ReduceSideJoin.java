import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import  org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List ;
import java.util.Objects;
import java.lang.String;
import org.apache.hadoop.mapreduce.Counter;

/***
 * This programm illustrates Reduce side join. Content of Each input file is read and processed by separete mapper. For Personal info related 
 * input data , mapper PersonalInfoMapper is used . Similarly for employment info , mapper EmploymentInfoMapper is used. Each of the mapper 
 * emits employee-id as key along with tagged data. Reducer combine the data from both mapper and write to file system.This programme expects 
 * comma separeted input files. First input file in  personal info file  and second input file in employment info file. 
 *
 ***/

public class ReduceSideJoin {
      public     enum LogCounter {TOTAL_LOG, TOTAL_PRS, TOTAL_EMPL };
      private static  String [] ConcatenateStrings(String []str1 , String []str2)
      {
          String retString [] = new String [str1.length + str2.length];
          for ( int  count =  0 ; count < str1.length ; count++)
          {
             retString [count] = str1[count];
          }
          for ( int  count =  0 ; count < str2.length ; count++)
          {
             retString [str1.length + count] = str2[count];
          }
          return retString ;

      }
      private static  String joinedStringsWithDelim(String delim , String []strArr)
      {
          StringBuffer retString = null;
          retString  = new StringBuffer (strArr[0]);
          for ( int  count =  1 ; count < strArr.length ; count++)
          {
             retString.append(delim); 
             retString.append(strArr[count]); 
          }
          return new String (retString) ;
      }

  public static  class  CombinePersonalEmploymentInfo2 extends Reducer< Text, Text , Text, Text>{
      @Override
      public void  reduce(Text key , Iterable<Text> values, Context context) throws  IOException, InterruptedException{
	  String []PersonalInfo= null;
	  String []EmploymentInfo = null;
	  for ( Text value : values){
	      String []splits= value.toString().split(",");
	      if (splits[0].equals("Personal")){
		  PersonalInfo = Arrays.copyOfRange(splits, 1 , splits.length);
	      }
	      else {
		  EmploymentInfo = Arrays.copyOfRange(splits, 1, splits.length);
	      }
	  }
	  String []joinedString = ConcatenateStrings(PersonalInfo, EmploymentInfo);
	  String strOutput = joinedStringsWithDelim(",", joinedString);
	  Counter counter = context.getCounter(LogCounter.class.getName(),
		  LogCounter.TOTAL_LOG.toString());
	  counter.increment(1);
	  context.write(key, new Text(strOutput ));
      }
  }

    public  static class PersonalInfoMapper extends Mapper <LongWritable , Text, Text, Text> {
      public void map(LongWritable key, Text value , Context context)throws IOException, InterruptedException{
          String record = value.toString();
          String  split[] =  record.split(",");
          Text  txt = new Text ("Personal" + "," + joinedStringsWithDelim(",", Arrays.copyOfRange(split, 1, split.length))); 
          String str = txt.toString();
          context.write( new Text(split[0].trim()) ,txt );
	  Counter counter = context.getCounter(LogCounter.class.getName(),
		  LogCounter.TOTAL_PRS.toString());
	  counter.increment(1);
      }
    }

    public  static class  EmploymentInfoMapper extends  Mapper<LongWritable, Text, Text, Text>{
        public  void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
            String record = value.toString();
            String  split[] =  record.split(",");
            Text txt = new Text ("Employ" + "," + joinedStringsWithDelim(",", Arrays.copyOfRange(split, 1, split.length)));
            String str =  txt.toString();
            context.write( new Text(split[0].trim()) ,txt);
	  Counter counter = context.getCounter(LogCounter.class.getName(),
		  LogCounter.TOTAL_EMPL.toString());
	  counter.increment(1);
        }
    }

    public static void main (String []args ) throws IOException, InterruptedException, ClassNotFoundException{

        if ( args .length != 3){
            System.err.println("required arguments are missing . Please specify personal info file , Employment info file and output file path");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance();
        job.setJobName("Reduce side join illustration");
        job.setJarByClass(ReduceSideJoin.class);
        job.setReducerClass(CombinePersonalEmploymentInfo2.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PersonalInfoMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, EmploymentInfoMapper.class);
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath,true);
        System.exit(job.waitForCompletion(true)? 0 :1);

    }
}
