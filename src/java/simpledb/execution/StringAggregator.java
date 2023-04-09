package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;int gbfield;
    Type gbfieldtype; 
    int afield;
    Op what;
    HashMap<Field,Integer> result = new HashMap<Field, Integer>();
    HashMap<Field,Integer> count = new HashMap<Field, Integer>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */ 

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        /** Simpler implementation compared to IntegerAggregator based on the 
         * grouping field or the total data */
        if(gbfieldtype == Type.INT_TYPE){
            IntField key = (IntField) tup.getField(gbfield);
            if(what.toString() == "count"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, 0);
                    result.put(null, result.get(null) + 1);
                }    
                else{
                    result.putIfAbsent(key, 0);
                    result.put(key, result.get(key) + 1);
                }
            }
        }
        else{
            StringField key = (StringField)tup.getField(gbfield);
            if(what.toString() == "count"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, 0);
                    result.put(null, result.get(null) + 1);
                }    
                else{
                    result.putIfAbsent(key, 0);
                    result.put(key, result.get(key) + 1);
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        /** To process the data into iterator for different group fields or
         * the whole dataset together
         */
        ArrayList<Tuple> tup_lis = new ArrayList<Tuple>();
        TupleDesc td;
        if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING){
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            td = new TupleDesc(types);
            Tuple tp = new Tuple(td);
            tp.setField(0, new IntField((int)result.get(null)));
            tup_lis.add(tp);
        }
        else{
            Type[] types = new Type[2];
            types[0] = gbfieldtype;
            types[1] = Type.INT_TYPE;
            td = new TupleDesc(types);
            Tuple tp;
            for (Map.Entry<Field, Integer> set : result.entrySet()) {
                tp = new Tuple(td);
                tp.setField(0, set.getKey());
                tp.setField(1, new IntField((int)result.get(set.getKey())));                    
                tup_lis.add(tp);
            }
        }
        return new TupleIterator(td, tup_lis);
    }

}