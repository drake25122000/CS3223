/**
 * Sort Merge Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Attribute> leftatts;   // Join attributes in left table
    ArrayList<Attribute> rightatts;   // Join attributes in right table
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch leftbatch;              // Buffer blocks for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    Tuple lefttuple;
    Tuple righttuple;
    ArrayList<Tuple> leftpart;
    ArrayList<Tuple> rightpart;
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    int lpcurs;
    int rpcurs;
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached
    boolean isFirstLeft;
    boolean isFirstRight;

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes
     * * Materializes the right hand side into a file
     * * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftatts = new ArrayList<>();
        rightatts = new ArrayList<>();
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftatts.add(leftattr);
            rightatts.add(rightattr);
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        lpcurs = 0;
        rpcurs = 0;
        eosl = false;
        eosr = false;
        isFirstLeft = true;
        isFirstRight = true;
        left = new Sort(left, leftatts, OpType.SORT, numBuff);
        right = new Sort(right, rightatts, OpType.SORT, numBuff);

        if (!(left.open() && right.open())) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j;

        Tuple test;

        lefttuple = getLeftTuple();
        righttuple = getRightTuple();


        if (eosl || eosr) {
            return null;
        }

        outbatch = new Batch(batchsize);
        while (!outbatch.isFull() && !eosl && !eosr) {
            int comparisonResult = Tuple.compareTuples(lefttuple, righttuple, leftindex, rightindex);
            if (comparisonResult == 0) {

                if (lpcurs == 0 && rpcurs == 0) {
                    leftpart = partitionLeft();
                    rightpart = partitionRight();
                }
                for (i = lpcurs; i < leftpart.size(); i++) {
                    for (j = rpcurs; j < rightpart.size(); j++) {
                        lefttuple = leftpart.get(i);
                        righttuple = rightpart.get(j);
                        Tuple outtuple = lefttuple.joinWith(righttuple);
                        outbatch.add(outtuple);
                        if (outbatch.isFull()) {
                            if (i == leftpart.size() - 1 && j == rightpart.size() - 1) {  //case 1
                                lpcurs = 0;
                                rpcurs = 0;
                            } else if (i != leftpart.size() - 1 && j == rightpart.size() - 1) {  //case 2
                                lpcurs = i + 1;
                                rpcurs = 0;
                            } else if (i == leftpart.size() - 1 && j != rightpart.size() - 1) {  //case 3
                                lpcurs = i;
                                rpcurs = j + 1;
                            } else {
                                lpcurs = i;
                                rpcurs = j + 1;
                            }
                            return outbatch;
                        }
                    }
                    rpcurs = 0;
                }
                lpcurs = 0;
                lefttuple = getLeftTuple();
                righttuple = getRightTuple();
            } else if (comparisonResult == -1) {
                lefttuple = getLeftTuple();
            } else {
                righttuple = getRightTuple();
            }
        }
        return outbatch;
    }

    public ArrayList<Tuple> partitionLeft() {
        ArrayList<Tuple> leftpart = new ArrayList<>();
        leftpart.add(lefttuple);
        Tuple topart = getLeftTuple();
        while (!eosl && lefttuple.checkJoin(topart, leftindex, leftindex)) {
            leftpart.add(topart);
            topart = getLeftTuple();
        }
        lcurs--;
        return leftpart;
}

    public ArrayList<Tuple> partitionRight() {
        ArrayList<Tuple> rightpart = new ArrayList<>();
        rightpart.add(righttuple);
        Tuple topart = getRightTuple();
        while (!eosr && righttuple.checkJoin(topart, rightindex, rightindex)) {


            rightpart.add(topart);

            topart = getRightTuple();
        }
        rcurs--;
        return rightpart;
    }

    public Tuple getLeftTuple() {
        if (isFirstLeft || (leftbatch != null && lcurs == leftbatch.size())) {
            leftbatch = left.next();
            isFirstLeft = false;
            lcurs = 0;
        }
        if (leftbatch == null) {
            eosl = true;
            return null;
        }
        Tuple lt = leftbatch.get(lcurs);
        lcurs++;
        return lt;
    }

    public Tuple getRightTuple() {
        if (isFirstRight || (rightbatch != null && rcurs == rightbatch.size())) {
            rightbatch = right.next();
            isFirstRight = false;
            rcurs = 0;
        }
        if (rightbatch == null) {
            eosr = true;
            return null;
        }
        Tuple rt = rightbatch.get(rcurs);
        rcurs++;
        return rt;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        left.close();
        right.close();
        return true;
    }

}
