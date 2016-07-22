import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List ;
import java.util.Objects;
import java.lang.String;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.filecache.DistributedCache;
import java.util.*;
import java.io.*;

/***
 * This programm illustrates Map side join. Content of smallest file is distributed and hashmapped on each task node using 
 * distributed  cache mechanism. This programme expects comma separeted input files. First input file in  personal info file 
 * and second input file in employment info file. 
 *
 ** */
public class MapSideJoin {

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


    public  static class  EmploymentInfoMapper extends  Mapper<LongWritable, Text, Text, Text>{
	private  Map <String , String[]> personalInfoMap ;

	public void setup (Context context ){
	    try {

		Path []cacheFile = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if( cacheFile != null)
		{
		    setupPersonalInfoMap(cacheFile[0]);
		}
		else {
		    System.exit(1);
		}
	    }
	    catch(FileNotFoundException e){
		System.err.println("Cache File is not found");
		System.exit(1);

	    }catch (IOException e ){
		System.err.println("Error reading state file");
		System.exit(1);
	    }
	}


	private  void setupPersonalInfoMap(Path filePath) throws FileNotFoundException, IOException{
	    personalInfoMap = new HashMap<String, String[]>();

	    try{
		BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
		String line = null;
		while((line = bufferedReader.readLine()) != null) {
		    String []split = line.split(",");
		    personalInfoMap.put(split[0].trim(),Arrays.copyOfRange(split, 1, split.length));
		}
	    } catch(IOException ex) {
		System.err.println("Exception while reading stop words file: " + ex.getMessage());
	    }
	}

	public  void map(LongWritable key, Text value , Context context) throws IOException, InterruptedException{
	    String record = value.toString();
	    String  split[] =  record.split(",");
	    if (personalInfoMap.containsKey(split[0].trim()))
	    {
		String []joinedString = ConcatenateStrings(  personalInfoMap.get(split[0]) ,Arrays.copyOfRange(split, 1, split.length));
		String strOutput = joinedStringsWithDelim(",", joinedString);
		context.write( new Text(split[0].trim()) ,new Text(strOutput ));
	    }
	}
    }

    public static void main (String []args ) throws IOException, InterruptedException, ClassNotFoundException{

        if ( args .length != 3){
            System.err.println("required arguments are missing . Please specify personal info file , Employment info file and output file path");
            System.exit(1);
        }
        Job job = Job.getInstance();
        job.setJobName("Map side join illustration");
        job.setJarByClass(MapSideJoin.class);
        job.setReducerClass(Reducer.class);
        job.setMapperClass(EmploymentInfoMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
        FileInputFormat.setInputPaths(job, new Path(args[1]));
        Path outputPath = new Path(args[2]);
        FileOutputFormat.setOutputPath(job, outputPath);
        outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,true);
        System.exit(job.waitForCompletion(true)? 0 :1);

    }
}
