import java.io.IOException;
import org.apache.hadoop.io.* ;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.* ;

public class UFORecordValidationMapper extends  Mapper<LongWritable, Text, LongWritable, Text> {

@Override
    public void map(LongWritable key, Text value,Context context ) throws IOException, InterruptedException {
        String line = value.toString();
        if (validate(line))
            context.write(key, value);
    }

    private boolean validate(String str){
        String[] parts = str.split("\t") ;
        if (parts.length != 6)return false ;
        return true ;
    }
}
