package simpledb.execution;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

import simpledb.common.Type;
import simpledb.storage.Tuple;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByIndex;
    private Type groupByType;
    private int aggregateFieldIndex;
    private Op what;
    public ConcurrentHashMap<Integer, Integer> aggregator;
    private ArrayList<Integer> averageAndCountHelper;
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
        this.groupByIndex = gbfield;
        if (this.groupByType != Type.INT_TYPE){
            System.out.println("Group by type is not an integer!");
        }
        this.groupByType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.what = what;
        this.aggregator = new ConcurrentHashMap<Integer, Integer>();
        if (this.what == Op.AVG || this.what == Op.COUNT){
            this.averageAndCountHelper = new ArrayList<Integer>();
        }
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
        // You need to put things into your aggregator, but group it first.

        System.out.println("\n\n\n\n\n\n");
        Integer keyValue = Integer.valueOf(tup.getField(this.groupByIndex).toString());
        Integer aValue = Integer.valueOf(tup.getField(this.aggregateFieldIndex).toString());


        if (this.what == Op.SUM){
            // if the value already exists in the aggregator, then replace the old value with the new one.
            if (aggregator.containsKey(keyValue)){
                aggregator.replace(keyValue, aggregator.get(keyValue), aggregator.get(keyValue) + aValue);
            }
            // Otherwise, enter a new value into the aggregator
            else{
                aggregator.put(keyValue, aValue);
            }
        }

        else if (this.what == Op.MAX){
            // if the value already exists in the aggregator, then replace the old value with the new one.
            if (aggregator.containsKey(keyValue)){
                if (aggregator.get(keyValue) < aValue){
                    aggregator.replace(keyValue, aggregator.get(keyValue), aValue);
                }
            }
            // Otherwise, enter a new value into the aggregator
            else{
                aggregator.put(keyValue, aValue);
            }
        }

        else if (this.what == Op.MIN){
            // if the value already exists in the aggregator, then replace the old value with the new one.
            if (aggregator.containsKey(keyValue)){
                if (aggregator.get(keyValue) > aValue){
                    aggregator.replace(keyValue, aggregator.get(keyValue), aValue);
                }
            }
            // Otherwise, enter a new value into the aggregator
            else{
                aggregator.put(keyValue, aValue);
            }
        }

        else if (this.what == Op.AVG){
            this.averageAndCountHelper.add(aValue);
            Integer Average = 0;
            // Not using IntSummaryStatistics class for average calculation due to constructor overhead.
            for (Integer i: this.averageAndCountHelper){
                Average += i;
            }
            Average = Average / this.averageAndCountHelper.size();
            aggregator.put(keyValue, Average);
        }

        else if (this.what == Op.COUNT){
            this.averageAndCountHelper.add(aValue);
            aggregator.put(keyValue, this.averageAndCountHelper.size());
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
        //return this.aggregator.iterator();
        throw new Exception("Has not been implemented yet!");
    }

}
