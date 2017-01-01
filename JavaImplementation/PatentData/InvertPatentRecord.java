import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.*;
import  org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

/** This programme invert patent data. Final output of this programme is in the form of : 
*  citation-patent patent1,patent2,...
*  Input file to this programme is cite75_99.txt
**/




class MapPatentRecord extends Mapper<Text, Text, Text, Text> {

    public void map(Text key , Text value, Context context) throws IOException, InterruptedException
    {
        context.write(value, key);
    }
}

class ReducePatentRecord extends Reducer < Text, Text, Text, Text > {
    public void reduce (Text key, Iterable<Text>values , Context context) throws IOException, InterruptedException
    {
        String csv = "";
        Iterator <Text> itr = values.iterator();
        while (itr.hasNext()){
            if (csv.length() > 0){
                csv += ",";
            }
            csv += itr.next().toString();
        }
        context.write(key, new Text(csv));
    }
}
public class InvertPatentRecord extends Configured implements Tool {

    public int run (String [] args)throws Exception{
        Configuration conf = getConf();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator",",");
        Job job = Job.getInstance(conf, "invertpatentrecord");
        job.setJarByClass(InvertPatentRecord.class);
        job.setMapperClass(MapPatentRecord.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileSystem fout = FileSystem.get(conf);
        Path outPath = new Path(args[1]);
        if(fout.exists(outPath))
            fout.delete(outPath, true);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setReducerClass(ReducePatentRecord.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setJobName("Invert Patent Record");
        System.exit(job.waitForCompletion(true)? 0: 1);
        return 0;

    }

    public static  void main(String [] args) throws  Exception, InterruptedException, ClassNotFoundException{
        int res = ToolRunner.run(new Configuration(),new InvertPatentRecord(),args);

        System.exit(res);
    }
}

