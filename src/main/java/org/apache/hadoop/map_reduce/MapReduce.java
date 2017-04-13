package org.apache.hadoop.map_reduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapReduce {

    public static class MatrixConstructMapper
        extends Mapper<Object, Text, IntWritable, IntWritable>{
        private  IntWritable from = new IntWritable();
        private  IntWritable to = new IntWritable();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString(), " ");
        int nodeNum = 5;
        from.set(Integer.valueOf(itr.nextToken()));
        to.set(Integer.valueOf(itr.nextToken()));
        context.write(from, to);
    }
}




public static class MatrixConstructReducer
        extends Reducer<IntWritable,IntWritable,MyKeyPair,MyFloatValuePair2> {
    private  MyKeyPair myKeyPair = new MyKeyPair();
    private MyFloatValuePair2 myFloatValuePair2 = new MyFloatValuePair2();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException{
        int totalNodeNum = 5;
        Vector<Integer> tos = new Vector<Integer>();
        int nodeNum = 0;
        tos.setSize(totalNodeNum + 1);
        for(int i = 1; i <= totalNodeNum; ++i){
            tos.setElementAt(0, i);
        }
        for(IntWritable val : values){
            tos.setElementAt(1, val.get());
            nodeNum ++;
        }
        float beta = 0.8f;
        for(int i = 1; i <= totalNodeNum; ++i){
            //myValuePair1.set(1, key.get(), i);
            //myValuePair2.set(2, key.get(), i);
            myKeyPair.set(key.get(), i);


            Integer cur = tos.get(i);
            if(cur == 1){
                myFloatValuePair2.set(1, (float)cur / nodeNum * beta + (1-beta) / totalNodeNum);
                context.write(myKeyPair, myFloatValuePair2);
                myFloatValuePair2.set(2, (float)1 / totalNodeNum);
                context.write(myKeyPair, myFloatValuePair2);
            } else{
                myFloatValuePair2.set(1, (1-beta) / totalNodeNum);
                context.write(myKeyPair, myFloatValuePair2);
                myFloatValuePair2.set(2, (float)1 / totalNodeNum);
                context.write(myKeyPair, myFloatValuePair2);
            }
        }
    }
}

public static class PageRankMultiplyMapper
    extends Mapper<MyKeyPair, MyFloatValuePair2, MyKeyPair, MyFloatValuePair>{
    private MyKeyPair keyPair = new MyKeyPair();
    private MyFloatValuePair valuePair = new MyFloatValuePair();

    public void map(MyKeyPair key, MyFloatValuePair2 value, Context context
    ) throws IOException, InterruptedException {
        int nameValue = -1, index1 = -1, index2 = -1;
        float val;

        nameValue = value.getIndex1();

        index1 = key.getI();
        index2 = key.getK();
        val = value.getIndex2();


        for (int k = 1; k <= 5; k++) {
            if (nameValue == 1) {
                keyPair.set(index1, k);
                valuePair.set(nameValue, index2, val);
                /*message += "( " + String.valueOf(keyPair.getI()) + "," + String.valueOf(keyPair.getK()) + " ) ";
                message += "( " + String.valueOf(valuePair.getName()) + "," + String.valueOf(valuePair.getIndex1())
                        + "," +  String.valueOf(valuePair.getIndex2()) + " )\n";*/
                context.write(keyPair, valuePair);
            } else {
                keyPair.set(k, index2);
                valuePair.set(nameValue, index1, val);
                context.write(keyPair, valuePair);
                /*message += "( " + String.valueOf(keyPair.getI()) + "," + String.valueOf(keyPair.getK()) + " ) ";
                message += "( " + String.valueOf(valuePair.getName()) + "," + String.valueOf(valuePair.getIndex1())
                        + "," +  String.valueOf(valuePair.getIndex2()) + " )\n";*/
            }

        }

    }
}

public static class IntSumReducer
        extends Reducer<MyKeyPair,MyFloatValuePair, IntWritable, MyFloatValuePair> {
    private IntWritable myKey = new IntWritable();
    private  MyFloatValuePair myFloatValuePair = new MyFloatValuePair();


    public void reduce(MyKeyPair key, Iterable<MyFloatValuePair> values,
                       Context context
    ) throws IOException, InterruptedException{
        float sum = 0;
        int count = 0;
        HashMap<Integer, Float> hashMapM = new HashMap<Integer, Float>();
        HashMap<Integer, Float> hashMapN = new HashMap<Integer, Float>();

        for(MyFloatValuePair val : values){
            if(val.getName() == 1){
                hashMapM.put(val.getIndex1(), val.getIndex2());
            }else{
                hashMapN.put(val.getIndex1(), val.getIndex2());
            }
            count++;
        }
        for(int i = 1; i <= count / 2; i++){
            float curM = hashMapM.get(i);
            sum += curM * hashMapN.get(i);
            myKey.set(key.getI());
            myFloatValuePair.set(1, i, curM);
            context.write(myKey, myFloatValuePair);
        }


        myKey.set(key.getI());
        myFloatValuePair.set(2, key.getK(), sum);
        context.write(myKey, myFloatValuePair);
    }
}

public static class PageRankDeadEndPreventMapper
        extends Mapper<IntWritable, MyFloatValuePair, IntWritable, MyFloatValuePair> {


    public void map(IntWritable key, MyFloatValuePair value, Context context
    ) throws IOException, InterruptedException {
        context.write(key, value);
    }
}


public static class PageRankDeadEndPreventReducer
        extends Reducer<IntWritable, MyFloatValuePair, MyKeyPair, MyFloatValuePair2> {

        private  MyFloatValuePair2 myFloatValuePair2 = new MyFloatValuePair2();
        private MyKeyPair myKeyPair = new MyKeyPair();

    public void reduce(IntWritable key, Iterable<MyFloatValuePair> values,
                       Context context
    ) throws IOException, InterruptedException{
        HashMap<MyKeyPair, Float> hashMapM = new HashMap<MyKeyPair, Float>();
        HashMap<Integer, Float> hashMapN = new HashMap<Integer, Float>();
        int count = 0;
        float s = 0;
        for(MyFloatValuePair val : values){
            if(val.getName() == 1){
                MyKeyPair keyPair = new MyKeyPair(key.get(), val.getIndex1());
                if(!hashMapM.containsKey(keyPair)){
                    myFloatValuePair2.set(1, val.getIndex2());
                    context.write(keyPair,   myFloatValuePair2);
                    hashMapM.put(keyPair, val.getIndex2());
                }
            }else{
                hashMapN.put(val.getIndex1(), val.getIndex2());
                s += val.getIndex2();
                count ++;
            }
        }
        for(int i = 1; i <= count; ++i){
            float rNew = hashMapN.get(i) + (1 - s) / 2;
            myFloatValuePair2.set(2, rNew);
            myKeyPair.set(key.get(), i);
            context.write(myKeyPair, myFloatValuePair2);
        }
    }
}

public static class OutputMapper
        extends Mapper<MyKeyPair, MyFloatValuePair2, IntWritable, FloatWritable> {
    private  IntWritable myKey = new IntWritable();
    private  FloatWritable myValue = new FloatWritable();

    public void map(MyKeyPair key, MyFloatValuePair2 value, Context context
    ) throws IOException, InterruptedException {
        myKey.set(key.getI());
        myValue.set(value.getIndex2());
        context.write(myKey, myValue);
    }
}


public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 5) {
        System.err.println("Usage: map_reduce <in> <out>");
        System.exit(2);
    }

    ////////////////////////job1///////////////////////////

    Job job1 = new Job(conf, "matrix construct");
    job1.setJarByClass(MapReduce.class);
    job1.setMapperClass(MatrixConstructMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(MatrixConstructReducer.class);

    job1.setMapOutputKeyClass(IntWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);

    job1.setOutputKeyClass(MyValuePair.class);
    job1.setOutputValueClass(FloatWritable.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));

    ////////////////////loop start//////////////////////////

    ////////////////////////job2///////////////////////////

    Job job2 = new Job(conf, "page rank matrix multiply");
    job2.setJarByClass(MapReduce.class);
    job2.setMapperClass(PageRankMultiplyMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);

    job2.setMapOutputKeyClass(MyKeyPair.class);
    job2.setMapOutputValueClass(MyFloatValuePair.class);

    job2.setOutputKeyClass(IntWritable.class);
    job2.setOutputValueClass(MyFloatValuePair.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

    ////////////////////////job3///////////////////////////

    Job job3 = new Job(conf, "prevent dead end");
    job3.setJarByClass(MapReduce.class);
    job3.setMapperClass(PageRankDeadEndPreventMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job3.setReducerClass(PageRankDeadEndPreventReducer.class);

    job3.setMapOutputKeyClass(IntWritable.class);
    job3.setMapOutputValueClass(MyFloatValuePair.class);

    job3.setOutputKeyClass(MyKeyPair.class);
    job3.setOutputValueClass(MyFloatValuePair2.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job3, new Path(otherArgs[2]));
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[3]));

    /////////////////////job4/////////////////////////////
    Job job4 = new Job(conf, "output page rank");
    job4.setJarByClass(MapReduce.class);
    job4.setMapperClass(OutputMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    //job4.setReducerClass(PageRankDeadEndPreventReducer.class);

    /*job4.setMapOutputKeyClass(IntWritable.class);
    job4.setMapOutputValueClass(MyFloatValuePair.class);*/

    job4.setOutputKeyClass(IntWritable.class);
    job4.setOutputValueClass(FloatWritable.class);

    /*job.setOutputKeyClass(MyKeyPair.class);
    job.setOutputValueClass(MyValuePair.class);*/

    FileInputFormat.addInputPath(job4, new Path(otherArgs[3]));
    FileOutputFormat.setOutputPath(job4, new Path(otherArgs[4]));



    System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
