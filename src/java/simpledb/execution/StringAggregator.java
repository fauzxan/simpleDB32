package simpledb.execution;


import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.TupleIterator;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int groupByIndex;
    private Type groupByType;
    private int aggregateFieldIndex;
    private Op what;
    private ConcurrentHashMap<IntField, Integer> aggregator;
    private IntField keyValue;
    private HashMap<IntField, ArrayList<String>> countHelper;

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
        this.groupByIndex = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        if (what == Op.COUNT){
            this.what = what;
        }
        else if (what != Op.COUNT){
            throw new IllegalArgumentException("Operator passed is not Op.COUNT | StringAggregator() | StringAggregator.java");
        }
        this.aggregator = new ConcurrentHashMap<IntField, Integer>();
        this.countHelper = new HashMap<IntField, ArrayList<String>>();
        this.keyValue = new IntField(0);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        if(this.groupByIndex != NO_GROUPING && this.groupByType == Type.INT_TYPE) this.keyValue = (IntField) tup.getField(this.groupByIndex); // update key value if grouping exists
        else if (this.groupByIndex != NO_GROUPING && this.groupByType == Type.STRING_TYPE) this.keyValue = new IntField(tup.getField(this.groupByIndex).hashCode());

        StringField aValue = (StringField) tup.getField(this.aggregateFieldIndex);

        if (!this.countHelper.containsKey(this.keyValue)){
            this.countHelper.put(this.keyValue, new ArrayList<String>());
        }
//        this.countHelper.put(this.keyValue, aValue.getValue());
        ArrayList<String> newList = this.countHelper.get(this.keyValue);
        newList.add(aValue.getValue());
        this.countHelper.put(this.keyValue, newList);
        aggregator.put(this.keyValue, this.countHelper.get(this.keyValue).size());
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
        ArrayList<Tuple> tuples = new ArrayList<>();

        if (this.groupByIndex != NO_GROUPING){
            TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[]{
                    new TupleDesc.TDItem(Type.INT_TYPE, null),
                    new TupleDesc.TDItem(Type.INT_TYPE, null)
            };
            TupleDesc td = new TupleDesc(tdItems);

            for (IntField key: this.aggregator.keySet()) {
                Tuple t = new Tuple(td);
                t.setField(0, key);
                t.setField(1, new IntField(this.aggregator.get(key)));
                tuples.add(t);
            }
            return new TupleIterator(td, tuples);
        }

        else{
            TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[]{
                    new TupleDesc.TDItem(Type.INT_TYPE, null)
            };
            TupleDesc td = new TupleDesc(tdItems);
            Tuple t = new Tuple(td);
            t.setField(0, new IntField(aggregator.get(this.keyValue)));
            tuples.add(t);
            return new TupleIterator(td, tuples);
        }
    }

}
