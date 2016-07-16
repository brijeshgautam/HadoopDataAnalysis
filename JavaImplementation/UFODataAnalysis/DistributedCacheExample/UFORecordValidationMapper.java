import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UFORecordValidationMapper  extends  Mapper<LongWritable, Text, LongWritable, Text>{
    public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException{

        if (validate(value)){
            context.write(key, value);
        }
    }
    Boolean  validate( Text text){
        String [] arr = text.toString().split("\\t");
        if ( arr.length >= 6)
            return true ;
        else return false;
    }
}
