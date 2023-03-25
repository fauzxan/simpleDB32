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
import simpledb.storage.Field;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int groupByIndex;
    private Type groupByType;
    private int aggregateFieldIndex;
    private Op what;
    private ConcurrentHashMap aggregator;
    private IntField keyValue;
    private StringField keyValueS;
    private ArrayList<Integer> CountHelper;
    private HashMap averageHelper;
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
        this.groupByType = gbfieldtype;
        this.aggregateFieldIndex = afield;
        this.what = what;

        // Integer/ null group by initializations:
        if (gbfieldtype == Type.INT_TYPE || gbfieldtype == null){
            this.averageHelper = new HashMap<IntField, ArrayList<Integer>>();
            this.keyValue = new IntField(0);
            this.aggregator = new ConcurrentHashMap<IntField, Integer>();
        }
        // String group by initializations:
        else if (gbfieldtype == Type.STRING_TYPE){
            this.averageHelper = new HashMap<StringField, ArrayList<Integer>>();
            this.keyValueS = new StringField("~", 10000);
            this.aggregator = new ConcurrentHashMap<StringField, Integer>();
        }
        this.CountHelper = new ArrayList<Integer>();

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

        /**
         * Setting the keyvalue for insertion.
         *
         * Keyvalue can either be a string type or an int type. Therefore, we must update keyvalue if grouping is not
         * null.
         *
         * Aggregate field value can only be Integer type here.
         */
        if(this.groupByIndex != NO_GROUPING && this.groupByType == Type.INT_TYPE) this.keyValue = (IntField) tup.getField(this.groupByIndex);
        else if (this.groupByIndex != NO_GROUPING && this.groupByType == Type.STRING_TYPE) this.keyValueS = (StringField) tup.getField(this.groupByIndex);
        IntField aValue = (IntField) tup.getField(this.aggregateFieldIndex);

        /**
         If the groupByType is an INT, then the HashMap invoved has a different type signature.
         */
        if (this.groupByType == Type.INT_TYPE || this.groupByType == null){
            if (this.what == Op.SUM) {
                // if the value already exists in the aggregator, then replace the old value with the new one.
                if (aggregator.containsKey(keyValue)) {
                    aggregator.replace(keyValue, (Integer) aggregator.get(keyValue) + aValue.getValue());
                }
                // Otherwise, enter a new value into the aggregator
                else {
                    aggregator.put(keyValue, aValue.getValue());
                }
            } else if (this.what == Op.MAX) {
                // if the value already exists in the aggregator, then replace the old value with the new one.
                if (aggregator.containsKey(keyValue)) {
                    if (aggregator.get(keyValue) < aValue.getValue()) {
                        aggregator.replace(keyValue, aValue.getValue());
                    }
                }
                // Otherwise, enter a new value into the aggregator
                else {
                    aggregator.put(keyValue, aValue.getValue());
                }
            } else if (this.what == Op.MIN) {
                // if the value already exists in the aggregator, then replace the old value with the new one.
                if (aggregator.containsKey(keyValue)) {
                    if (aggregator.get(keyValue) > aValue.getValue()) {
                        aggregator.replace(keyValue, aValue.getValue());
                    }
                }
                // Otherwise, enter a new value into the aggregator
                else {
                    aggregator.put(keyValue, aValue.getValue());
                }
            } else if (this.what == Op.AVG) {
                if (!this.averageHelper.containsKey(this.keyValue)) {
                    this.averageHelper.put(this.keyValue, new ArrayList<Integer>());
                }
                ArrayList<Integer> newList = this.averageHelper.get(this.keyValue);
                newList.add(aValue.getValue());
                this.averageHelper.put(this.keyValue, newList);

                Integer Average = 0;
                for (Integer i : this.averageHelper.get(this.keyValue)) {
                    Average += i;
                }
                Average = Average / this.averageHelper.get(this.keyValue).size();
                aggregator.put(keyValue, Average);
            } else if (this.what == Op.COUNT) {
                this.CountHelper.add(aValue.getValue());
                aggregator.put(keyValue, this.CountHelper.size());
            }
        }

        /**
        If the groupByType is a STRING, then the HashMap invoved has a different type signature.
         */
        if (this.groupByType == Type.STRING_TYPE){
            if (this.what == Op.SUM) {
                // if the value already exists in the aggregator, then replace the old value with the new one.
                if (aggregator.containsKey(keyValueS)) {
                    aggregator.replace(keyValueS, aggregator.get(keyValueS) + aValue.getValue());
                }
                // Otherwise, enter a new value into the aggregator
                else {
                    aggregator.put(keyValueS, aValue.getValue());
                }
            } else if (this.what == Op.MAX) {
                // if the value already exists in the aggregator, then replace the old value with the new one.
                if (aggregator.containsKey(keyValueS)) {
                    if (aggregator.get(keyValueS) < aValue.getValue()) {
                        aggregator.replace(keyValueS, aValue.getValue());
                    }
                }
                // Otherwise, enter a new value into the aggregator
                else {
                    aggregator.put(keyValueS, aValue.getValue());
                }
            } else if (this.what == Op.MIN) {
                // if the value already exists in the aggregator, then replace the old value with the new one.
                if (aggregator.containsKey(keyValueS)) {
                    if (aggregator.get(keyValueS) > aValue.getValue()) {
                        aggregator.replace(keyValueS, aValue.getValue());
                    }
                }
                // Otherwise, enter a new value into the aggregator
                else {
                    aggregator.put(keyValueS, aValue.getValue());
                }
            } else if (this.what == Op.AVG) {
                if (!this.averageHelper.containsKey(this.keyValueS)) {
                    this.averageHelper.put(this.keyValueS, new ArrayList<Integer>());
                }
                ArrayList<Integer> newList = this.averageHelper.get(this.keyValueS);
                newList.add(aValue.getValue());
                this.averageHelper.put(this.keyValueS, newList);

                Integer Average = 0;
                for (Integer i : this.averageHelper.get(this.keyValueS)) {
                    Average += i;
                }
                Average = Average / this.averageHelper.get(this.keyValueS).size();
                aggregator.put(keyValueS, Average);
            } else if (this.what == Op.COUNT) {
                this.CountHelper.add(aValue.getValue());
                aggregator.put(keyValueS, this.CountHelper.size());
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
        ArrayList<Tuple> tuples = new ArrayList<>();

        if (this.groupByType == Type.INT_TYPE){
            if (this.groupByIndex != NO_GROUPING) {
                TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[]{
                        new TupleDesc.TDItem(Type.INT_TYPE, null),
                        new TupleDesc.TDItem(Type.INT_TYPE, null)
                };
                TupleDesc td = new TupleDesc(tdItems);

                for (IntField key : this.aggregator.keySet()) {
                    Tuple t = new Tuple(td);
                    t.setField(0, key);
                    t.setField(1, new IntField(this.aggregator.get(key)));
                    tuples.add(t);
                }

                return new TupleIterator(td, tuples);
            } else {
                TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[]{
                        new TupleDesc.TDItem(Type.INT_TYPE, null)
                };
                TupleDesc td = new TupleDesc(tdItems);
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(aggregator.get(this.keyValue)));
                tuples.add(t);
                return new TupleIterator(td, tuples);
            }
            // throw new UnsupportedOperationException("please implement me for lab2");
        }
        else{
            if (this.groupByIndex != NO_GROUPING) {
                TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[]{
                        new TupleDesc.TDItem(Type.STRING_TYPE, null),
                        new TupleDesc.TDItem(Type.INT_TYPE, null)
                };
                TupleDesc td = new TupleDesc(tdItems);

                for (StringField key : this.aggregator.keySet()) {
                    Tuple t = new Tuple(td);
                    t.setField(0, key);
                    t.setField(1, new IntField(this.aggregator.get(key)));
                    tuples.add(t);
                }

                return new TupleIterator(td, tuples);
            } else {
                TupleDesc.TDItem[] tdItems = new TupleDesc.TDItem[]{
                        new TupleDesc.TDItem(Type.INT_TYPE, null)
                };
                TupleDesc td = new TupleDesc(tdItems);
                Tuple t = new Tuple(td);
                t.setField(0, new IntField(aggregator.get(this.keyValueS)));
                tuples.add(t);
                return new TupleIterator(td, tuples);
            }
        }
    }
}
