package org.apache.hadoop.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;



public class MyDoubleWritable implements WritableComparable<MyDoubleWritable>{
	
	private DoubleWritable value;
	private Text id;
	
	public MyDoubleWritable(){
		setValue(new DoubleWritable(0));
		setId(new Text("111"));
	}
	
	public MyDoubleWritable(MyDoubleWritable o){
		setValue(o.value);
		setId(o.id);
	}
	
	public Text getId() {
		return id;
	}

	public void setId(Text id) {
		this.id = id;
	}
	
	public MyDoubleWritable(DoubleWritable v, Text i){
		setValue(v);
		setId(i);
	}
	
	public DoubleWritable getValue() {
		return value;
	}

	public void setValue(DoubleWritable value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		value.readFields(in);
		id.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		value.write(out);
		id.write(out);
	}

	public boolean equals(MyDoubleWritable o) {
		if((this.value == o.value) && (this.id == o.id)){
			return true;
		}else{
			return false;
		}
	}
	
	public int hashCode() {
		return value.hashCode() * 13;
	}
	
	@Override
	public int compareTo(MyDoubleWritable o) {
		// TODO Auto-generated method stub
		if(value.compareTo(o.value) != 0){
			return -value.compareTo(o.value);
		}else if (id.compareTo(o.id) != 0){
			return id.compareTo(o.id);
		}else{
			return 0;
		}
	}
	


}
