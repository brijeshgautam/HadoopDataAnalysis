import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Counter;

import java.io.IOException;

public class UFORecordValidationMapper  extends  Mapper<LongWritable, Text, LongWritable, Text>{

    public enum LineCounters {BAD_LINES,TOO_MANY_TABS,TOO_FEW_TABS};
  
    public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException{

        if (validate(value, context)){
            context.write(key, value);
        }
    }
    Boolean  validate( Text text, Context context){
        String [] arr = text.toString().split("\\t");
        if ( arr.length != 6)
        {
            if(arr.length < 6)
            {
                Counter counter = context.getCounter(LineCounters.class.getName(),
                LineCounters.TOO_FEW_TABS.toString());
                counter.increment(1);
            }
            else
            {
                Counter counter = context.getCounter(LineCounters.class.getName(),
                LineCounters.TOO_MANY_TABS.toString());
                counter.increment(1);
            }

            Counter counter = context.getCounter(LineCounters.class.getName(),
            LineCounters.BAD_LINES.toString());
            counter.increment(1);
            return false;
        }
        return true ;
    }
}
