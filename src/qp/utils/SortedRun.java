/**
 * Tuple container class
 **/

package qp.utils;

import java.util.*;
import java.io.*;

/**
 * Tuple - a simple object which holds an ArrayList of data
 */
public class SortedRun implements Serializable {

    public ArrayList<Tuple> tupleList;

    public SortedRun(ArrayList<Tuple> temp) {
        tupleList = temp;
    }

    public boolean isEmpty() {
         return tupleList.isEmpty();
    }

    public Tuple poll(int index) {
        return tupleList.remove(index);
    }

    public Tuple get(int index) {
        return tupleList.get(index);
    }

    public void add(Tuple toAdd) {
        tupleList.add(toAdd);
    }

    public int size() {
        return tupleList.size();
    }

    @Override
    public String toString(){
        String output = "";

        for (int i = 0 ; i < size() ; i++) {
            output = output + get(i) + "\n";
        }

        return output;
    }
}
