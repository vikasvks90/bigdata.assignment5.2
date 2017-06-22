/**
 * <h1>SortTV</h1>
 * This class is implementing WritableComparable interface to give reducer 
 * input key in sorted manner based on company name and tv size
 * */
package mapreduce.assignment5.task7;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
 
public class SortTV implements WritableComparable<SortTV> {
 
    private String company;
    private int size;
    
    public String getCompany() {
        return company;
    }
 
    public int getSize() {
        return size;
    }
    
    public void set(String company, int size) {
        this.company = company;
        this.size = size;
    }
 
    @Override
    // This method is inherited from Writable interface and will be used to deserialize the fields of the object from ‘in’.
    public void readFields(DataInput in) throws IOException {
    	company = in.readUTF();
    	size = in.readInt();
    }
 
    @Override
    // This method is inherited from Writable interface and will be used to serialize the fields of the object to ‘out’.
    public void write(DataOutput out) throws IOException {
    	out.writeUTF(company);
    	out.writeInt(size);
    }
 
    @Override
    //this method is inherited from Object class and is used to print user defined objects accordingly
    public String toString() {
        return company + "\t" + size;
    }
 
    @Override
    // This method is inherited from Comparable interface and will be used to sort the input keys to reducer
    //based on hashcode and equals method
    public int compareTo(SortTV tv) {
        int cmp = this.company.compareTo(tv.company);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return (-1) * (this.size - tv.getSize());
    }
 
    @Override
    public int hashCode(){
        return company.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof SortTV)
        {
        	SortTV tv = (SortTV) o;
            return company.equalsIgnoreCase(tv.company) && (size == tv.getSize());
        }
        return false;
    }
  
}
