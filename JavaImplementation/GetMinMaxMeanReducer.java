
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import  java.io.IOException;

/**
 * Created by ANIMESH on 02-07-2016.
 */
public class GetMinMaxMeanReducer extends Reducer<Text, LongWritable,Text,Text> {
    public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
    {
        Long total = new Long(0);
        Long minDuration = new Long(Long.MAX_VALUE);
        Long maxDuration = new Long(0);
        Long count = new Long(0);

        for (LongWritable val : values)
        {
            total = total + val.get();
            count += 1;
            if (maxDuration < val.get()){
                maxDuration = val.get();
            }
            else if (val.get() < minDuration){
                minDuration = val.get();
            }
        }
        String str = new String ( minDuration.toString() +"\t" + maxDuration.toString() + "\t" + ( total/(count * 1.0)) + "\t" + total + "\t" + count);
        context.write(key, new Text(str));
    }
}
