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
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    int gbfield;
    Type gbfieldtype; 
    int afield;
    Op what;
    HashMap<Field,Integer> result = new HashMap<Field, Integer>();
    HashMap<Field,Integer> count = new HashMap<Field, Integer>();

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        /** Given below is the implementation of aggregation of Integer field. 
         * As it can be observed, there are muliple ways to aggregate int, 
         * therefore the multiple if statements to cater to the queries needs */

        IntField value = (IntField) tup.getField(afield);

        if(gbfieldtype == Type.INT_TYPE){
            IntField key = (IntField) tup.getField(gbfield);
            if(what.toString() == "min"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, Integer.MAX_VALUE);
                    result.put(null, Math.min(result.get(null), value.getValue()));
                }    
                else{
                    result.putIfAbsent(key, Integer.MAX_VALUE);
                    result.put(key, Math.min(result.get(key), value.getValue()));
                }
            }
            if(what.toString() == "max"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, Integer.MIN_VALUE);
                    result.put(null, Math.max(result.get(null), value.getValue()));
                }    
                else{
                    result.putIfAbsent(key, Integer.MIN_VALUE);
                    result.put(key, Math.max(result.get(key), value.getValue()));
                }
            }
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
            if(what.toString() == "sum"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, 0);
                    result.put(null, result.get(null) + value.getValue());
                }    
                else{
                    result.putIfAbsent(key, 0);
                    result.put(key, result.get(key) + value.getValue());
                }
            }
            if(what.toString() == "avg"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, 0);
                    count.putIfAbsent(null, 0);
                    result.put(null, result.get(null) + value.getValue());
                    count.put(null, count.get(null) + 1);
                }    
                else{
                    result.putIfAbsent(key, 0);
                    count.putIfAbsent(key, 0);
                    result.put(key, result.get(key) + value.getValue());
                    count.put(key, count.get(key) + 1);
                }
            }
        }
        else{
            StringField key = (StringField)tup.getField(gbfield);
            if(what.toString() == "min"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, Integer.MAX_VALUE);
                    result.put(null, Math.min(result.get(null), value.getValue()));
                }    
                else{
                    result.putIfAbsent(key, Integer.MAX_VALUE);
                    result.put(key, Math.min(result.get(key), value.getValue()));
                }
            }
            if(what.toString() == "max"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, Integer.MIN_VALUE);
                    result.put(null, Math.max(result.get(null), value.getValue()));
                }    
                else{
                    result.putIfAbsent(key, Integer.MIN_VALUE);
                    result.put(key, Math.max(result.get(key), value.getValue()));
                }
            }
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
            if(what.toString() == "sum"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, 0);
                    result.put(null, result.get(null) + value.getValue());
                }    
                else{
                    result.putIfAbsent(key, 0);
                    result.put(key, result.get(key) + value.getValue());
                }
            }
            if(what.toString() == "avg"){
                if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING) {
                    result.putIfAbsent(null, 0);
                    count.putIfAbsent(null, 0);
                    result.put(null, result.get(null) + value.getValue());
                    count.put(null, count.get(null) + 1);
                }    
                else{
                    result.putIfAbsent(key, 0);
                    count.putIfAbsent(key, 0);
                    result.put(key, result.get(key) + value.getValue());
                    count.put(key, count.get(key) + 1);
                }
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        /** As some aggregation queries is done based on a field column or for the 
         * whole dataset and average aggregation is calculated differently from the 
         * rest, this code block helps to make sure data processing is handled 
         * based on case-to-case */

        ArrayList<Tuple> tup_lis = new ArrayList<Tuple>();
        TupleDesc td;
        if(gbfieldtype == null || gbfield == Aggregator.NO_GROUPING){
            Type[] types = new Type[1];
            types[0] = Type.INT_TYPE;
            td = new TupleDesc(types);
            Tuple tp = new Tuple(td);
            if(what.toString() == "avg"){
                tp.setField(0, new IntField((int)(result.get(null)/count.get(null))));
                tup_lis.add(tp);
            }
            else{
                tp.setField(0, new IntField((int)result.get(null)));
                tup_lis.add(tp);
            }
        }
        else{
            Type[] types = new Type[2];
            types[0] = gbfieldtype;
            types[1] = Type.INT_TYPE;
            td = new TupleDesc(types);
            Tuple tp;
            if(what.toString() == "avg"){
                for (Map.Entry<Field, Integer> set : result.entrySet()) {
                    tp = new Tuple(td);
                    tp.setField(0, set.getKey());
                    tp.setField(1, new IntField((int)(result.get(set.getKey())/count.get(set.getKey()))));
                    tup_lis.add(tp);
                }
            }
            else{
                for (Map.Entry<Field, Integer> set : result.entrySet()) {
                    tp = new Tuple(td);
                    tp.setField(0, set.getKey());
                    tp.setField(1, new IntField((int)result.get(set.getKey())));
                    tup_lis.add(tp);
                }
            }
        }
        return new TupleIterator(td, tup_lis);
    }
}
