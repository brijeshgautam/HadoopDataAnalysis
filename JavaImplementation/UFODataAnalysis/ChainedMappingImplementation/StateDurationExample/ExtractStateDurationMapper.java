import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ExtractStateDurationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Pattern stateRegex = Pattern.compile( ".*,\\s*([a-zA-Z]{2}).*");
    Pattern durationRegex =  Pattern.compile("(\\d+)\\s+((min)|(sec)|(hour))", Pattern.CASE_INSENSITIVE) ;
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
                        context.write(new Text(stateMatcher.group(1).toUpperCase()), new LongWritable(duration));

                    }

                }
            }
        }

    }
}
