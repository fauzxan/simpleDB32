package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int numFields;

    private TDItem[] tdAr;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;

        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new TDIterator();
    }
    private class TDIterator implements Iterator<TDItem>{
        private int p = 0;
        @Override
        public boolean hasNext(){
            return tdAr.length > p;
        }
        @Override
        public TDItem next(){
            if(!hasNext()){
                throw new NoSuchElementException("hasNext threw an error | TupleDesc.java | TDIterator.next()");
            }
            return tdAr[p++];
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        numFields = typeAr.length;
        tdAr = new TDItem[numFields];

        for (int i=0; i<numFields; i++){
            tdAr[i] = new TDItem(typeAr[i], fieldAr[i]);
        }

    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this(typeAr, new String[typeAr.length]);
    }
    public TupleDesc(TDItem[] tdItems) {
        if (tdItems == null || tdItems.length == 0) {
            throw new IllegalArgumentException("tdItems parameter was null or empty | TupleDesc.java | TupleDesc(TDItem[])");
        }
        this.tdAr = tdItems;
        this.numFields = tdItems.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.numFields;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */

    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= numFields || i<0) {
            throw new NoSuchElementException("TupleDesc parameter out of bounds | TupleDesc.java | getFieldName(i)");
        }
        else{
            return tdAr[i].fieldName;
        }
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i >= numFields || i<0) {
            throw new NoSuchElementException("TupleDesc parameter out of bounds | TupleDesc.java | getFieldType(i)");
        }
        else {
            return tdAr[i].fieldType;
        }
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name== null){
            throw new NoSuchElementException("Name parameter was null | TupleDesc.java | fieldNameToIndex(name)");
        }
        String fieldName;
        for(int i=0; i< tdAr.length; i++){
           
            if((fieldName = tdAr[i].fieldName) != null && fieldName.equals(name)){
                return i;
            }
               
        }
        throw new NoSuchElementException("fieldName not found | TupleDesc.java | fieldNameToIndex(name)");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size =0;
        for(int i =0; i < tdAr.length; i++){
            size += tdAr[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TDItem[] tdItems1 = td1.tdAr;
        TDItem[] tdItems2 = td2.tdAr;
        int len1 = tdItems1.length;
        int len2 = tdItems2.length;
        TDItem[] resultItems = new TDItem[len1 + len2];
        System.arraycopy(tdItems1, 0, resultItems, 0, len1);
        System.arraycopy(tdItems2, 0, resultItems, len1, len2);
        return new TupleDesc(resultItems);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        return o.hashCode() == this.hashCode();
    }

    /**
     *
     * @return hashCode() value of the string version of the TupleDesc object
     */
    public int hashCode() {
        return this.toString().hashCode();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer res = new StringBuffer();
        res.append("Fields: ");
        for (TDItem tdItem : tdAr) {
            res.append(tdItem.toString() + ", ");
        }
        res.append(numFields + " Fields in total");
        return res.toString();
    }
}