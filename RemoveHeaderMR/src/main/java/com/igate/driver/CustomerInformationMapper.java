
package com.igate.driver;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class CustomerInformationMapper extends Mapper<LongWritable, Text, Text, Text>
{
	String header;
	JobContext thiscontext;
	String filename;
	FileSplit fileSplit = null;
	
	
	
	// Header Columns
	String custid_header="";
	String date_header="";
	String os_header="";
	String version_header="";
	String question1_header;
	String question2_header;
	
	//Header Indexes
	int custid_headerIndx=0;
	int date_headerIndx=1;
	int os_headerIndx=2;
	int version_Indx=3;
	int question1_headerIndx=4;
	int question2_headerIndx=5;
	MultipleOutputs<Text, Text> mos;
	
	
   
    public void setup(Context context)
    		throws IOException, InterruptedException {
    	thiscontext=context;
    	fileSplit = (FileSplit) context.getInputSplit();
    	filename = fileSplit.getPath().getName();
    	mos = new MultipleOutputs<Text, Text>(context);
    	while (context.nextKeyValue()) {
            if (context.getCurrentKey().get() != 0L) {
            	 header= context.getConfiguration().get(filename);
            	 System.out.println("header in mapper"+header);
            	// context.getConfiguration().unset("HEADER");
            	// context.getConfiguration().clear();
            	// context.getConfiguration().reloadConfiguration();
            	 thiscontext.getConfiguration().setIfUnset(filename,header);
                map(context.getCurrentKey(), context.getCurrentValue(), context);
            } else {
                System.out.println(" Skipping the header: " + context.getCurrentValue());
                header=context.getCurrentValue().toString();
               // System.out.println("header in mapper"+context.getCurrentValue().toString());
                thiscontext.getConfiguration().setIfUnset(filename,header);
            }
        }
    	// TODO Auto-generated method stub
    	
    }
    /*
     * The map method runs once for each line of text in the input file. The method receives a key of type LongWritable, a value of type Text, and a Context object.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
     //System.out.println("header in map -->"+context.getConfiguration().get("HEADER"));
    //context.getConfiguration().set("HEADER",header);
	String line = value.toString();
	//String line = value.toString(); --> This get count without Ignore
	/*
	 * The line.split("\\W+") call uses regular expressions to split the line up by non-word characters.
	 * 
	 * If you are not familiar with the use of regular expressions in Java code, search the web for "Java Regex Tutorial."
	 */
	
	// Use the string tokeniser
	
	/*StringTokenizer words= new StringTokenizer( value.toString().toLowerCase().replaceAll("\\p {Punct}|\\d", ""));
	while (words.hasMoreElements())
	{
		String word=words.nextToken();
	}*/
	
	String[] data=line.split(",");
	String custid="";
	String date="";
	String os="";
	String verion="";
	String question1="";
	String question2="";
	
	String[] headerColumn=header.split(",");
	
	custid_header=headerColumn[custid_headerIndx];
    date_header=headerColumn[date_headerIndx];
	os_header=headerColumn[os_headerIndx];
	version_header=headerColumn[version_Indx];
	question1_header=headerColumn[question1_headerIndx];
	question2_header=headerColumn[question2_headerIndx];
	    if (data.length > 0)
	    {

		/*
		 * Call the write method on the Context object to emit a key and a value from the map method.
		 */
	    	//System.out.println("value to red"+line);
	    	//context.getConfiguration().set("HEADER",header);
	    	custid=data[custid_headerIndx];
    		date=data[date_headerIndx];
    		os=data[os_headerIndx];
    		verion=data[version_Indx];
    		
    		question1=data[question1_headerIndx];
    		question2=data[question2_headerIndx];
    		String custinfo=custid+","+date+","+os+","+verion;
        	mos.write("CustomerInfo",NullWritable.get(), custinfo);
		  
		    for(int i=4;i<(data.length);i++)
		    {
		    	generateResponses(custid, date, os, headerColumn[i], data[i], context);
		    }
		//context.write(new Text(word)), new IntWritable(1));  --> Gets duplicates without case
	    }
	
    }
    
    public void generateResponses(String custid, String date, String os, String question, String answer, Context context) throws IOException, InterruptedException{
    	String responseinfo=custid+","+date+","+question+","+answer;
    	
    	mos.write("CustomerResponse",NullWritable.get(), responseinfo);
    	
    }
    @Override
    
    protected void cleanup(
    		Mapper<LongWritable, Text, Text, Text>.Context context)
    		throws IOException, InterruptedException {
    	// TODO Auto-generated method stub
    	//super.cleanup(context);
    	mos.close();
    	
    }
}