/**
 * This is base class for all the join operators
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedLoopJoin extends Join {

    static int filenum = 0;         // To get unique filenum for this operation
    int batchsize;                  // Number of tuples per out batch
    ArrayList<Integer> leftindex;   // Indices of the join attributes in left table
    ArrayList<Integer> rightindex;  // Indices of the join attributes in right table
    String rfname;                  // The file name where the right table is materialized
    Batch outbatch;                 // Buffer page for output
    Batch[] leftbatch;              // Buffer blocks for left input stream
    Batch rightbatch;               // Buffer page for right input stream
    ObjectInputStream in;           // File pointer to the right hand materialized file

    int lbcurs;                     // Cursor for left side block
    int lcurs;                      // Cursor for left side buffer
    int rcurs;                      // Cursor for right side buffer
    boolean eosl;                   // Whether end of stream (left table) is reached
    boolean eosr;                   // Whether end of stream (right table) is reached

    public BlockNestedLoopJoin(Join jn) {
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
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /** because right stream is to be repetitively scanned
         ** if it reached end, we have to start new scan
         **/
        eosr = true;

        /** Right hand side table is to be materialized
         ** for the Block Nested Loop join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/
            filenum++;
            rfname = "BNLJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedLoopJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    /**
     * from input buffers selects the tuples satisfying join condition
     * * And returns a page of output tuples
     **/
    public Batch next() {
        int i, j, k;
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            int lastIndex = 0;
            if (lcurs == 0 && eosr == true) {
                /** new left pages are to be fetched**/
                leftbatch = new Batch[getNumBuff() - 2];

                for (int point = 0 ; point < leftbatch.length ; point++) {
                    leftbatch[point] = (Batch) left.next();
                    if (leftbatch[point] == null) break;
                    lastIndex++;
                }

                if (leftbatch[0] == null) {
                    eosl = true;
                    return outbatch;
                }
                /** Whenever a new left page came, we have to start the
                 ** scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedLoopJoin:error in reading the file");
                    System.exit(1);
                }

            }
            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (i = lbcurs; i < lastIndex; ++i){
                        for (j = lcurs; j < leftbatch[i].size(); ++j) {
                            for (k = rcurs; k < rightbatch.size(); ++k) {
                                Tuple lefttuple = leftbatch[i].get(j);
                                Tuple righttuple = rightbatch.get(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (i == leftbatch.length - 1) {
                                            if (j == leftbatch[i].size() - 1 && k == rightbatch.size() - 1) {  //case 1
                                                lbcurs = 0;
                                                lcurs = 0;
                                                rcurs = 0;
                                            } else if (j != leftbatch[i].size() - 1 && k == rightbatch.size() - 1) {  //case 2
                                                lbcurs = i;
                                                lcurs = j + 1;
                                                rcurs = 0;
                                            } else if (j == leftbatch[i].size() - 1 && k != rightbatch.size() - 1) {  //case 3
                                                lbcurs = i;
                                                lcurs = j;
                                                rcurs = k + 1;
                                            } else {
                                                lbcurs = i;
                                                lcurs = j;
                                                rcurs = k + 1;
                                            }
                                        } else {
                                            if (j == leftbatch[i].size() - 1 && k == rightbatch.size() - 1) {  //case 1
                                                lbcurs = i + 1;
                                                lcurs = 0;
                                                rcurs = 0;
                                            } else if (j != leftbatch[i].size() - 1 && k == rightbatch.size() - 1) {  //case 2
                                                lbcurs = i;
                                                lcurs = j + 1;
                                                rcurs = 0;
                                            } else if (j == leftbatch[i].size() - 1 && k != rightbatch.size() - 1) {  //case 3
                                                lbcurs = i;
                                                lcurs = j;
                                                rcurs = k + 1;
                                            } else {
                                                lbcurs = i;
                                                lcurs = j;
                                                rcurs = k + 1;
                                            }
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lbcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedLoopJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedLoopJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedLoopJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
