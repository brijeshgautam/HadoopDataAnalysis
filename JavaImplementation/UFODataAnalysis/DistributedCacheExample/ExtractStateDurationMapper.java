import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.*;
import  org.apache.hadoop.fs.Path;
public class ExtractStateDurationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Pattern stateRegex = Pattern.compile( ".*,\\s*([a-zA-Z]{2}).*");
    Pattern durationRegex =  Pattern.compile("(\\d+)\\s+((min)|(sec)|(hour))", Pattern.CASE_INSENSITIVE) ;
    private  Map <String, String> stateNames ;

    public void configure (Job job){
        try {

               Path []cacheFile = job.getLocalCacheFiles();
               setupStateMap(cacheFile[0].toString());
        }
        catch(FileNotFoundException e){
            System.err.println("Cache File is not found");
            System.exit(1);

        }catch (IOException e ){
            System.err.println("Error reading state file");
            System.exit(1);
        }
    }
    private  void setupStateMap(String fileName) throws FileNotFoundException, IOException{
        Map<String, String> state = new HashMap<String, String>();
        BufferedReader br = new BufferedReader(new FileReader(fileName));
        String line = null;
        while ((line = br.readLine()) != null){
            String []split = line.split("\\t");
            state.put(split[0].trim().toUpperCase(), split[1].trim());
        }
        stateNames =state;
    }

    public void map( LongWritable key, Text text, Context context)throws IOException, InterruptedException{

        String []parts = text.toString().split("\\t");
        if (parts[2].trim().length()> 0){
            Matcher stateMatcher = stateRegex.matcher(parts[2].trim());
            if (stateMatcher.find()){
                if (parts[4].trim().length() > 0) {
                    Matcher durationMatcher = durationRegex.matcher(parts[4].trim());
                    if (durationMatcher.find()) {

                        Long duration = Long.parseLong(durationMatcher.group(1));
                        if (durationMatcher.group(2).trim().toLowerCase().startsWith("min")){
                            duration = duration * 60;
                        }
                        else if ( durationMatcher.group(2).trim().toLowerCase().startsWith("hour")){
                            duration = duration * 3600;
                        }
                        String stateAbbr = stateMatcher.group(1).toUpperCase();
                        context.write(new Text(lookupState(stateMatcher.group(1).toUpperCase())), new LongWritable(duration));

                    }

                }
            }
        }

    }

    private  String lookupState(String state){
        String stateFullName = stateNames.get(state);
        return stateFullName == null ?"OTHER" : stateFullName;
    }
}
