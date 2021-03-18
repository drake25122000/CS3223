/**
 * To projec out the required attributes from the result
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;

import java.util.ArrayList;

public class Project extends Operator {

    Operator base;                 // Base table to project
    ArrayList<Attribute> attrset;  // Set of attributes to project
    int batchsize;                 // Number of tuples per outbatch
	int max = -999999; // starting max value for MAX function
	int min = 999999; // starting min value for MIN function
	int count = 0; // variable to store COUNT
	boolean aggPresent = false; // flag to check if the SELECT statements contain aggregate functions
    /**
     * The following fields are requied during execution
     * * of the Project Operator
     **/
    Batch inbatch;
    Batch outbatch;

    /**
     * index of the attributes in the base operator
     * * that are to be projected
     **/
    int[] attrIndex;
	int[] aggIndex; // index of attribute that is involved in aggregate operation
	Object[] aggData; // Array to store all aggregated data

    public Project(Operator base, ArrayList<Attribute> as, int type) {
        super(type);
        this.base = base;
        this.attrset = as;
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


    /**
     * Opens the connection to the base operator
     * * Also figures out what are the columns to be
     * * projected from the base operator
     **/
    public boolean open() {
        /** set number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        if (!base.open()) return false;

        /** The following loop finds the index of the columns that
         ** are required from the base operator
         **/
        Schema baseSchema = base.getSchema();
        
		attrIndex = new int[attrset.size()];
		aggIndex = new int[attrset.size()];
		aggData = new Object[attrset.size()];
		
        for (int i = 0; i < attrset.size(); ++i) {
            Attribute attr = attrset.get(i);

            /*if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggragation is not implemented.");
                System.exit(1);
            }*/

            int index = baseSchema.indexOf(attr.getBaseAttribute());
            attrIndex[i] = index;
		
			// Following lines check the aggregate type of the attributes and assign a value to aggIndex[] 
			// A starting value is also initialised in aggData[] depending on the aggType.
			// AVG and SUM functions are not implemented.
			if (attr.getAggType() == Attribute.AVG) {
				System.err.println("AVG function is not implemented");
				System.exit(1);
			}
			if (attr.getAggType() == Attribute.SUM) {
				System.err.println("SUM function is not implemented");
				System.exit(1);
			}
			
			if (attr.getAggType() == Attribute.MAX) {
				aggIndex[i] = 1;
				aggData[i] = (Object)max;
			} 
			else if(attr.getAggType() == Attribute.MIN) {
				aggIndex[i] = 2;
				aggData[i] = (Object)min;
			}
			else if(attr.getAggType() == Attribute.COUNT) {
				aggIndex[i] = 3;
				aggData[i] = (Object)count;
			}
			else {
				aggIndex[i] = 0;
			}
        }
        return true;
    }

    /**
     * Read next tuple from operator
     */
    public Batch next() {
        outbatch = new Batch(batchsize);
        /** all the tuples in the inbuffer goes to the output buffer **/
        inbatch = base.next();
        
		
	    	// This will write out the aggregate results when there is no more tuples to be read
		if(aggPresent) {
			if (inbatch == null) {
				ArrayList<Object>aggResult = new ArrayList<>();
				for (int i = 0; i<attrset.size(); i++) {
					aggResult.add(aggData[i]);
				}
				Tuple aggTuple = new Tuple(aggResult);
				outbatch.add(aggTuple);
				aggPresent = false;
					
				return outbatch;
			}
		}
			
		if (inbatch == null) {	
			return null;
        }
	    
        for (int i = 0; i < inbatch.size(); i++) {
            Tuple basetuple = inbatch.get(i);
            ArrayList<Object> present = new ArrayList<>();
            
			for (int j = 0; j < attrset.size(); j++) {
				Object data = basetuple.dataAt(attrIndex[j]);
	            
				if(aggIndex[j] == 0) {
					present.add(data); // we project out the attribute if there is no aggregate function on it
				}
				else if(aggIndex[j] == 1) { // Calculation for MAX function
					aggPresent = true;
						try {
							if((int)data > (int)aggData[j]) {
								aggData[j] = data;
							}	
						} catch (Exception e){
								System.out.println("Attribute to be aggregated is not of type Int");
								System.exit(1);
						}
				}
				else if(aggIndex[j] == 2) { // Calculation for MIN function
					aggPresent = true;
						try {
							if((int)data <(int)aggData[j]) {
								aggData[j] = data;
							}
						} catch (Exception e) {
								System.out.println("Attribute to be aggregated is not of type Int");
								System.exit(1);
						}
				}
				else if(aggIndex[j] == 3) { // Calculation for COUNT function
					aggPresent = true;
					aggData[j] = (int)aggData[j] + 1;
				}
			}	
			if (!aggPresent) { // Only project out attributes if there is no aggregate function
				Tuple outtuple = new Tuple(present);
				outbatch.add(outtuple);
			}
			
		}
		
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
        Project newproj = new Project(newbase, newattr, optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }

}
