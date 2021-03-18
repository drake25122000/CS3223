/**
 * Sort Operation
 **/

package qp.operators;

import qp.utils.*;
import java.util.*;
import java.io.*;

public class Sort extends Operator {

    Operator base;                      // Base operator
    Condition con;                      // Select condition
    int batchsize;                      // Number of tuples per outbatch
    ArrayList<Integer> attIndexList;    // List of attribute index to be sort
    ArrayList<Attribute> attList;       // List of attribute to be sort
    static int filenum = 0;             // To get unique filenum for this operation
    static int outfilenum = 0;          // Pointer for the outputfile num
    static int iteration = 0;           // Pointer for the iteration
    static int maxNumTuples = 0;        // Max number of tuple in sorted run after merging
    ObjectInputStream in;               // File pointer to the materialized file
    ObjectOutputStream out;             // File pointer to the materialized file
    TupleReader tr;                     // tuple reader
    int numSortedRuns;                  // Number of sorted runs
    String rfname;                      // The file name where the right table is materialized
    static int desc = 1;

    /**
     * The following fields are required during
     * * execution of the sort operator
     **/
    Batch[] inbatch;                    // This is the current input buffer
    Batch outbatch;                     // This is the current output buffer
    int start;                          // Cursor position in the input buffer
    Schema schema;                      // Schema of th
    int numOfBuff;                      // Number of buffer available

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

    public static void setDesc() {
        desc = -1;
    }

    public static int getNumberOfPasses() {
        return 1;
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

        // To get the index of every attribute in attribute list
        for (Attribute att : attList) {
            this.attIndexList.add(schema.indexOf(att));
        }

        // Initialize temp page to receive data from base
        Batch temp;

        // Initialize arraylist of tuples to be insert to SortedRuns
        ArrayList<Tuple> tempTuples = new ArrayList<>();


        System.out.println("_________");

        // Generate sorted runs
        while ((temp = base.next()) != null) {
            // Initialize target file name
            rfname = "Stemp-" + String.valueOf(iteration) + "-" + String.valueOf(filenum);


            for (int k = 0; k < numOfBuff; k++) {
                for (int p = 0; p < temp.size(); p++) {
                    tempTuples.add(temp.get(p));
                }


                if ( k < numOfBuff - 1 ) {
                    if ((temp = base.next()) == null) break;
                }
            }

            // In memory sort
            Collections.sort(tempTuples, (tup1, tup2) -> compareTuples(tup1, tup2));

            // Update number of max tuples after merging
            maxNumTuples = tempTuples.size() > maxNumTuples ? tempTuples.size() : maxNumTuples;

            // Start tuple writer
            TupleWriter tw = new TupleWriter(rfname, tempTuples.size());
            tw.open();

            //Write tuple to file
            for (int i = 0 ; i < tempTuples.size() ; i++) {
                tw.next(tempTuples.get(i));
            }


            tw.close();
            numSortedRuns++;
            filenum++;
            tempTuples = new ArrayList<>();

        }


        filenum = 0;

        // To check if number of sorted runs greater than 1
        while (numSortedRuns > 1) {

            // To pointer for sorted runs that have been merged
            int countSortedRuns = 0;

            // Temporary variable for number of sorted runs
            int tempNumSortedRuns = numSortedRuns;

            // Reinitiate numsorted runs to 0
            numSortedRuns = 0;
            while (countSortedRuns < tempNumSortedRuns) {
                ArrayList<String> indexes = new ArrayList<>();
                for (int i = 0 ; i < numOfBuff - 1 ; i++) {
                    if (countSortedRuns == tempNumSortedRuns) {
                        break;
                    } else {
                        // To add sorted run file name to the list
                        rfname = "Stemp-" + String.valueOf(iteration) + "-" + String.valueOf(countSortedRuns);
                        indexes.add(rfname);
                        // To count number of sorted runs that are merged in this process
                        countSortedRuns++;
                    }
                }
                //System.out.println(indexes);
                numSortedRuns++;
                mergeRuns(indexes);

                // Delete files generated in previous iteration
                for (String index : indexes) {
                    File file = new File(index);
                    file.delete();
                }
            }
            // Increment iteration
            iteration++;

            // Reset oufilenum for new iteration
            outfilenum = 0;

        }

        // Reset filenum for new iteration
        filenum = 0;

        // Update rf name to the latest sorted run file
        rfname = "Stemp-" + String.valueOf(iteration) + "-" + String.valueOf(filenum);

        // Open a new sorted run file
        tr = new TupleReader(rfname, batchsize);
        tr.open();

        return true;

    }

    public boolean isOverMergeProcess (ArrayList<Batch> batches) {

        for (int i = 0; i < batches.size() ; i++) {
            if (!batches.get(i).isEmpty()) {
                return false;
            }
        }

        return true;
    }

    public void mergeRuns(ArrayList<String> indexes) {
        //System.out.println(indexes);

        // Initiate number of buffer
        ArrayList<Batch> batches = new ArrayList<>();

        // Initiate number of tuple reader
        ArrayList<TupleReader> readers = new ArrayList<>();

        // Initiate number of out batch after merge
        Batch tempOutBatch = new Batch(batchsize);

        TupleWriter tw = new TupleWriter("Stemp-" + String.valueOf(iteration + 1) + "-" + String.valueOf(outfilenum), maxNumTuples);

        tw.open();

        // Pointer to which batch has the smallest value
        int minimum = 0;

        // Count how many tuple merged
        int count = 0;

        //Initialize batches and readers
        for (int i = 0 ; i < indexes.size() ; i++) {
            TupleReader tempReader = new TupleReader(indexes.get(i), batchsize);
            tempReader.open();
            Batch temp = new Batch(batchsize);

            while (!temp.isFull()){

                if ( tempReader.isEOF() ) {
                    break;
                }

                temp.add(tempReader.next());
            }

            /*for (int k = 0 ; k < temp.size(); k++) {
                System.out.println(temp.get(k));
            }*/
            batches.add(temp);
            readers.add(tempReader);
        }

        // Check if all of the tuple in the sorted runs have been polled
        while (!isOverMergeProcess(batches)) {
            // Check if sorted run with index minimum is already empty
            while (batches.get(minimum).isEmpty()) {
                minimum++;
            }

            // Get the smallest tuple from the first element of every sorted runs
            for (int i = minimum + 1 ; i < batches.size() ; i++) {
                if (!batches.get(i).isEmpty() && compareTuples(batches
                        .get(i).get(0), batches.get(minimum).get(0)) < 0 ) {
                    minimum = i;
                }
            }

            Tuple tempTuple = batches.get(minimum).poll();
            tempOutBatch.add(tempTuple);

            if (tempOutBatch.isFull()) {
                while (!tempOutBatch.isEmpty()) {
                    //System.out.println(tempOutBatch.get(0));
                    tw.next(tempOutBatch.poll());
                    count++;
                }
            }

            if ( !readers.get(minimum).isEOF() ) {
                batches.get(minimum).add(readers.get(minimum).next());
            }

            minimum = 0;
        }

        while (!tempOutBatch.isEmpty()) {
            //System.out.println(tempOutBatch.get(0));
            tw.next(tempOutBatch.poll());
            count++;
        }

        //System.out.println("-----");

        maxNumTuples = count > maxNumTuples ? count : maxNumTuples;

        tw.close();
        outfilenum++;

    }

/*    public void mergeRuns(ArrayList<SortedRun> sortedruns) {

        SortedRun temp = new SortedRun(new ArrayList<Tuple>());
        int minimum = 0;

        // Check if all of the tuple in the sorted runs have been polled
        while (!isOverMergeProcess(sortedruns)) {

            // Check if sorted run with index minimum is already empty
            while (sortedruns.get(minimum).isEmpty()) {
                minimum++;
            }

            // Get the smallest tuple from the first element of every sorted runs
            for (int i = minimum + 1 ; i < sortedruns.size() ; i++) {
                if (!sortedruns.get(i).isEmpty() && compareTuples(sortedruns.get(i).get(0), sortedruns.get(minimum).get(0)) < 0 ) {
                    minimum = i;
                }
            }
            Tuple tempTuple = sortedruns.get(minimum).poll(0);
            temp.add(tempTuple);
            minimum = 0;
        }

        try {
            // Write temp object to file with rfname as the name of the file
            out.writeObject(temp);
        } catch (IOException io) {
            System.out.println("Sort: Error writing to temporary file");
            System.exit(1);
        }
    }*/

    /**
     * returns a batch of tuples that satisfies the
     * * condition specified on the tuples coming from base operator
     * * NOTE: This operation is performed on the fly
     **/
    public Batch next() {

        if (tr.isEOF()) {
            return null;
        }

        /** An output buffer is initiated **/
        outbatch = new Batch(batchsize);

        /** keep on checking the incoming pages until
         ** the output buffer is full & the final sorted run is empty
         **/
        while (!outbatch.isFull() && !tr.isEOF()) {
            outbatch.add(tr.next());
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

        return res * desc;
    }

}
