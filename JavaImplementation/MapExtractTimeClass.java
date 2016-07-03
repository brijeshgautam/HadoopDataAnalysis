
/**
 * Created by ANIMESH on 02-07-2016.
 */
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.Pattern;
import java.io.IOException;
import java.util.regex.Matcher;


public class MapExtractTimeClass extends Mapper<LongWritable, Text, Text, LongWritable> {

        private static Pattern durationPattern = Pattern.compile ("(\\d+)\\s+((min)|(sec)|(hour))") ;

        public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException
        {
            String line = value.toString();
            String[] fields = line.split("\t") ;
            String shape = fields[3].trim() ;
            String duration = fields[4].trim();

            if (shape.length() > 0 && duration.length() > 0)
            {
                Matcher matcher = durationPattern.matcher(duration) ;
                if (matcher.find() )
                {
                    int time  = Integer.parseInt( matcher.group(1));
                    if (matcher.group(2).toLowerCase().startsWith("min")){
                        time *= 60;
                    }
                    else if (matcher.group(2).toLowerCase().startsWith("hour")){
                        time *= 3600;
                    }
                    context.write(new Text(shape.toLowerCase()), new LongWritable(time));
                }
            }
        }
}
