package cs435.customObjects;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TFValues implements WritableComparable {

    private double tfValue;
    private String unigram;
    private int frequency;

    public TFValues(){
        tfValue = 0;
        unigram = "";
        frequency = 0;
    }

    public TFValues(String gram, double tfValue, int frequency) {
        this.tfValue = tfValue;
        unigram = gram;
        this.frequency = frequency;
    }

    public int getFrequency(){
        return frequency;
    }

    public String getUnigram(){
        return unigram;
    }


    @Override
    public int compareTo(Object o) {
        if(o instanceof articleUnigram) {
            if(tfValue == ((TFValues) o).tfValue && this.unigram.equals(((TFValues) o).unigram)){
                return 0;
            }
            else {
                return this.unigram.compareTo(((TFValues) o).unigram) + (int)(this.tfValue - ((TFValues) o).tfValue);
            }
        }
        else{
            return -1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(frequency);
        dataOutput.writeDouble(tfValue);
        dataOutput.writeInt(unigram.length());
        dataOutput.writeBytes(unigram);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //read document id
        frequency = dataInput.readInt();
        tfValue = dataInput.readDouble();
        //read in unigram
        int length = dataInput.readInt();
        byte [] buffer = new byte[length];
        dataInput.readFully(buffer,0,length);
        unigram = new String(buffer);
    }

    @Override
    public String toString(){
        return unigram + "\t" + tfValue + "\t";
    }

}
