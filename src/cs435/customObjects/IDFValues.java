package cs435.customObjects;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IDFValues implements WritableComparable {

    private double IDFValue;
    private String documentID;

    public IDFValues(){
        IDFValue = 0;
        documentID = "";
    }

    public IDFValues(String gram, double tfValue) {
        this.IDFValue = tfValue;
        documentID = gram;
    }


    public String getDocumentID(){
        return documentID;
    }

    public double getIDFValue() {
        return IDFValue;
    }

    @Override
    public int compareTo(Object o) {
        if(o instanceof articleUnigram) {
            if(IDFValue == ((IDFValues) o).IDFValue && this.documentID.equals(((IDFValues) o).documentID)){
                return 0;
            }
            else {
                return this.documentID.compareTo(((IDFValues) o).documentID) + (int)(this.IDFValue - ((IDFValues) o).IDFValue);
            }
        }
        else{
            return -1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(IDFValue);
        dataOutput.writeInt(documentID.length());
        dataOutput.writeBytes(documentID);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //read document id
        IDFValue = dataInput.readDouble();
        //read in documentID
        int length = dataInput.readInt();
        byte [] buffer = new byte[length];
        dataInput.readFully(buffer,0,length);
        documentID = new String(buffer);
    }

    @Override
    public String toString(){
        return documentID + "\t" + IDFValue + "\t";
    }

}
