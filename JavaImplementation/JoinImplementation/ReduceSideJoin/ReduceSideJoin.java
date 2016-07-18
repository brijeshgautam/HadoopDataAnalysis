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

public class ReduceSideJoin {
      private static  String [] ConcatenateStrings(String []str1 , String []str2)
      {
          String retString [] = new String [str1.length + str2.length];
          for ( int  count =  0 ; count < str1.length ; count++)
          {
             retString [count] = str1[count];
          }
          for ( int  count =  0 ; count < str2.length ; count++)
          {
             retString [str1.length + count] = str1[count];
          }
          return retString ;

      }
      private static  String joinedStringsWithDelim(String delim , String []strArr)
      {
          StringBuffer retString = null;
          retString  = new StringBuffer (strArr[0]);
          for ( int  count =  1 ; count < strArr.length ; count++)
          {
             retString.append("\t"); 
             retString.append(strArr[count]); 
          }
          return new String (retString) ;
      }

  private static  class  CombinePersonalEmploymentInfo extends Reducer< Text, Iterable<Text> , Text, Text>{
        public void  reduce(Text key , Iterable<Text>values, Context context) throws  IOException, InterruptedException{

            String []PersonalInfo= null;
            String []EmploymentInfo = null;
            for ( Text value : values){
                String []splits= value.toString().split("\t");
                if (splits[0].equals("Personal")){
                  PersonalInfo = Arrays.copyOfRange(splits, 1 , splits.length);
                }
                else {
                    EmploymentInfo = Arrays.copyOfRange(splits, 1, splits.length);
                }
            }
            String []joinedString = ConcatenateStrings(PersonalInfo, EmploymentInfo);
            String strOutput = joinedStringsWithDelim("\t", joinedString);
            context.write(key, new Text(strOutput ));
        }
  }

    private static class PersonalInfoMapper extends Mapper <LongWritable , Text, Text, Text> {
      public void map(LongWritable key, Text value , Context context)throws IOException, InterruptedException{
          String record = value.toString();
          String  split[] =  record.split("\t");
          context.write( new Text(split[0]) , new Text ("Personal" + "\t" + joinedStringsWithDelim("\t", Arrays.copyOfRange(split, 1, split.length))) );
      }
    }

    private  static class  EmploymentInfoMapper extends  Mapper<LongWritable, Text, Text, Text>{
        public  void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
            String record = value.toString();
            String  split[] =  record.split("\t");
            context.write( new Text(split[0]) , new Text ("Employ" + "\t" + joinedStringsWithDelim("\t", Arrays.copyOfRange(split, 1, split.length))) );
        }
    }

    public static void main (String []args ) throws IOException, InterruptedException, ClassNotFoundException{

        if ( args .length != 3){
            System.err.println("required arguments are missing . Please specify personal info file , Employment info file and output file path");
            System.exit(1);
        }
        Job job = Job.getInstance();
        Configuration conf = job.getConfiguration();
        job.setJobName("Reduce side join illustration");
        job.setJarByClass(ReduceSideJoin.class);
        job.setReducerClass(CombinePersonalEmploymentInfo.class);
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
