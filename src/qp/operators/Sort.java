/**
 * Select Operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.*;
import java.io.*;

public class Sort extends Operator {

    Operator base;                      // Base operator
    Condition con;                      // Select condition
    int batchsize;                      // Number of tuples per outbatch
    ArrayList<SortedRun> sr;            // List of sorted runs;
    ArrayList<Integer> attIndexList;    // List of attribute to be sort
    ArrayList<Attribute> attList;
    static int filenum = 0;             // To get unique filenum for this operation
    ObjectInputStream in;               // File pointer to the materialized file
    ObjectOutputStream out;             // File pointer to the materialized file
    int numSortedRuns;                  // Number of sorted runs
    String rfname;                      // The file name where the right table is materialized
    SortedRun finalSortedRun;         // Final single sorted run from out

    /**
     * The following fields are required during
     * * execution of the sort operator
     **/
    Batch[] inbatch;                    // This is the current input buffer
    Batch outbatch;                     // This is the current output buffer
    int start;                          // Cursor position in the input buffer
    Schema schema;
    int numOfBuff;
    SortedRun finale;

    /**
     * constructor
     **/
    public Sort(Operator base, ArrayList<Attribute> attList ,  int type, int numOfBuff) {
        super(type);
        this.base = base;
        this.schema = base.getSchema();
        this.con = con;
        this.numOfBuff = numOfBuff;
        this.attList = attList;

    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public void setNumOfBuff(int buff) {
        this.numOfBuff = buff;
    }

    /**
     * Opens the connection to the base operator
     **/
    public boolean open() {


        if (!base.open()) {
            return false;
        }

        /** Set number of tuples per page**/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;
        attIndexList = new ArrayList<>();
        numSortedRuns = 0;

        for (Attribute att : attList) {
            this.attIndexList.add(schema.indexOf(att));
        }

        sr = new ArrayList<SortedRun>();
        Batch temp;

        ArrayList<Tuple> tempTuples = new ArrayList<>();


        rfname = "Stemp-" + String.valueOf(filenum);

        try {
            out = new ObjectOutputStream(new FileOutputStream(rfname));
            // Generate sorted runs
            while ((temp = base.next()) != null) {
                for (int k = 0; k < numOfBuff; k++) {
                    for (int p = 0; p < temp.size(); p++) {
                        tempTuples.add(temp.get(p));
                    }
                    if ((temp = base.next()) == null) break;
                }

                Collections.sort(tempTuples, (tup1, tup2) -> compareTuples(tup1, tup2));

                out.writeObject(new SortedRun(tempTuples));
                numSortedRuns++;
                tempTuples = new ArrayList<>();

            }

            out.close();
        } catch (IOException io) {
            System.out.println("Sort: Error writing to temporary file");
            return false;
        }

        int[] index = new int[numOfBuff - 1];



        try {
            int it = 0;
            while (numSortedRuns > 1) {

                rfname = "Stemp-" + String.valueOf(filenum);
                in = new ObjectInputStream(new FileInputStream(rfname));
                System.out.println("Ini iteration ke:" + it + " file ke: " + filenum);

                numSortedRuns = 0;
                SortedRun tempSr;
                ArrayList<SortedRun> srToMerge = new ArrayList<>();
                filenum++;

                rfname = "Stemp-" + String.valueOf(filenum);
                out = new ObjectOutputStream(new FileOutputStream(rfname));
                try {
                    tempSr = (SortedRun) in.readObject();
                    while (tempSr != null) {

                        int count = 0;
                        while (count < numOfBuff - 1 && tempSr != null) {
                            //System.out.println(tempSr);
                            srToMerge.add(tempSr);
                            count++;
                            tempSr = (SortedRun) in.readObject();
                        }

                        numSortedRuns++;
                        mergeRuns(srToMerge);
                    }
                } catch (EOFException e) {
                    try {
                        numSortedRuns++;
                        mergeRuns(srToMerge);
                        in.close();
                    } catch (IOException io) {
                        System.out.println("Sort: Error in reading temporary file");
                    }
                } catch (IOException io) {
                    System.out.println("Sort: Error in reading temporary file");
                }
                out.close();
                it++;
                System.out.println(numSortedRuns);

            }
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("Sort: Error in reading temporary file");
            }
        } catch (ClassNotFoundException c) {
            System.out.println("Sort: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("Sort: Error in reading temporary file");
            System.exit(1);
        }

        filenum--;
        rfname = "Stemp-" + String.valueOf(filenum);
        try {
            in = new ObjectInputStream(new FileInputStream(rfname));
            finalSortedRun = (SortedRun) in.readObject();
        } catch (EOFException e) {
            try {
                in.close();
            } catch (IOException io) {
                System.out.println("Sort: Error in reading temporary file");
            }
        } catch (ClassNotFoundException c) {
            System.out.println("Sort: Error in deserialising temporary file ");
            System.exit(1);
        } catch (IOException io) {
            System.out.println("Sort: Error in reading temporary file");
            System.exit(1);
        }
        return true;

    }

    public boolean isOverMergeProcess (ArrayList<SortedRun> sortedruns) {

        for (int i = 0; i < sortedruns.size() ; i++) {
            if (!sortedruns.get(i).isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void mergeRuns(ArrayList<SortedRun> sortedruns) {

        SortedRun temp = new SortedRun(new ArrayList<Tuple>());
        int minimum = 0;
        while (!isOverMergeProcess(sortedruns)) {
            while (sortedruns.get(minimum).isEmpty()) {
                minimum++;
            }

            for (int i = minimum + 1 ; i < sortedruns.size() ; i++) {
                if (!sortedruns.get(i).isEmpty() && compareTuples(sortedruns.get(i).get(0), sortedruns.get(minimum).get(0)) < 0 ) {
                    minimum = i;
                }
            }
            Tuple tempTuple = sortedruns.get(minimum).poll(0);
            temp.add(tempTuple);
            minimum = 0;
        }

        System.out.println(temp);
        System.out.println("______________________ haha");

        try {
            out.writeObject(temp);
        } catch (IOException io) {
            System.out.println("Sort: Error writing to temporary file");
            System.exit(1);
        }
    }

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/
    public Batch next() {
        int i = 0;
        if (finalSortedRun.isEmpty()) {
            close();
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        /** keep on checking the incoming pages until
         ** the output buffer is full
         **/
        while (!outbatch.isFull()) {
            outbatch.add(finalSortedRun.poll(0));
        }
        return outbatch;

    }

    /**
     * closes the output connection
     * * i.e., no more pages to output
     **/
    public boolean close() {
        base.close();    // Added base.close
        return true;
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        Sort newsort = new Sort(newbase, attList, OpType.SORT, numOfBuff);
        newsort.setSchema((Schema) newbase.getSchema().clone());
        return newsort;
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
