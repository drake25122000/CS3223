/**
 * Distinct Operation
 **/

package qp.operators;

import qp.utils.*;

public class Distinct extends Operator {

    Operator base;
    ArrayList<Attribute> attributes;

    public Distinct(Operator base, int type, ArrayList<Attribute> attributes) {
        super(type);
        this.base = base;
        this.attributes = attributes;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    public boolean open() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return true;
    }

    public Batch next() {
        System.err.println("Abstract interface cannot be used.");
        System.exit(1);
        return null;
    }

    public boolean close() {
        return true;
    }

    public Object clone() {
        return new Operator(optype);
    }

}