/**
 * Distinct operation
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import qp.operators.Sort;
import java.util.ArrayList;
import java.util.HashMap;

public class Distinct extends Operator {

    Operator base;                      // Base table to project
    ArrayList<Attribute> attrset;       // Set of attributes to project
    ArrayList<Integer> attIndexList;    // List of attribute index to be sort
    int batchsize;                      // Number of tuples per outbatch
    int numOfBuff;                      // Number of buffers
    Tuple trackTuple;                   // To track if tuple is exist in the list

    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    boolean eos;                    // end of stream
    Batch inbatch;
    Batch outbatch;               // Buffer block for output stream

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;

    public Distinct(Operator base, ArrayList<Attribute> as, int type, int numOfBuff) {
        super(type);
        this.base = base;
        this.attrset = as;
        this.numOfBuff = numOfBuff;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public ArrayList<Attribute> getProjAttr() {
        return attrset;
    }

    public void setNumOfBuff(int buff) {
        this.numOfBuff = buff;
    }


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        outbatch = new Batch(batchsize);
        eos = false;

        attIndexList = new ArrayList<>();

        base = new Sort(getBase() , attrset, OpType.SORT, numOfBuff);

        // To get the index of every attribute in attribute list
        for (Attribute att : attrset) {
            this.attIndexList.add(schema.indexOf(att));
        }

        if (!base.open()) return false;


        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {

        if (eos) {
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        Batch temp;

        while ( ( (temp = base.next()) != null ) && !outbatch.isFull() ) {
            for (int i = 0 ; i < temp.size() ; i++) {
                Tuple fromBase = temp.get(i);
                if (trackTuple == null) {
                    outbatch.add(fromBase);
                    trackTuple = fromBase;
                    System.out.println(fromBase);
                } else {
                    if ( compareTuples(fromBase, trackTuple) != 0 ) {
                        outbatch.add(fromBase);
                        trackTuple = fromBase;
                        System.out.println(fromBase);

                    }
                }
            }
        }

        if (temp == null && outbatch.isEmpty()) {
            eos = true;
            return null;
        }

        //while ()

        return outbatch;

    }

    /**
     * Close the operator
     */
    public boolean close() {
        inbatch = null;
        base.close();
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attrset.size(); ++i)
            newattr.add((Attribute) attrset.get(i).clone());
        Distinct newDist = new Distinct(newbase, newattr, optype, numOfBuff);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newDist.setSchema(newSchema);
        return newDist;
    }


    public int compareTuples(Tuple tuple1, Tuple tuple2) {
        int res = 0;

        for (Integer attIndex : attIndexList) {
            res = Tuple.compareTuples(tuple1, tuple2, attIndex);
            if (res != 0) {
                break;
            }
        }

        return res;
    }
}
