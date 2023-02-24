package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

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
        // some code goes here
        return new TDIterator();
    }
    private class TDIterator implements Iterator<TDItem>{
        private int p = 0;
        @Overrirde
        public boolean hasNext(){
            return tdAr.length > p;
        }
        @Overrirde
        public TDItem next(){
            if(hasNext()){
                throw new NoSuchElementException();
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
        // some code goes here
        this.numFields = typeAr.length;
        this.fieldTypes = new Type[numFields];
        this.fieldNames = new String[numFields];

        for (int i=0; i<this.numFields; i++){
            fieldTypes[i] = typeAr[i];
            fieldNames[i] = fieldAr[i];
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
        // some code goes here
        this.numFields = typeAr.length;
        this.fieldTypes = new Type[numFields];
        this.fieldNames = new String[numFields];

        for (int i=1; i<this.numFields; i++){
            fieldTypes[i] = typeAr[i];
            fieldNames[i] = null;
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
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
        // some code goes here
        if (i >= this.numFields)
            throw new NoSuchElementException();
        return this.fieldNames[i];
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
        // some code goes here
        if (i >= this.numFields)
            throw new NoSuchElementException();
        return this.fieldTypes[i];
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
        // some code goes here
        for(int i=0; i<= this.numFields; i++){
            if(fieldNames[i]== null){
                throw new NoSuchElementException();
            }
            if(fieldNames[i].equals(name))
                return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size =0;
        for(int i =0; i <= this.numFields; i++){
            size += fieldTypes[i].getLength();
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
        // some code goes here
        Type mergedTypes[] = new Type[td1.numFields() + td2.numFields()];
        String mergedNames[] = new String[td1.numFields() + td2.numFields()];
        for(int i=0; i<td1.numFields(); i++){
            mergedTypes[i] = td1.getFieldType(i);
            mergedNames[i] = td1.getFieldName(i);
        }
        for(int j=0; j<td2.numFields(); j++){
            mergedTypes[td1.numFields() + j] = td2.getFieldType(j);
            mergedNames[td1.numFields() + j] = td2.getFieldName(j);
        }
        TupleDesc mergedtuple = new TupleDesc(mergedTypes, mergedNames);
        return mergedtuple;
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
        // some code goes here
        TupleDesc otd;
        if(o == null){
            return false;
        }
        try{
            otd = (TupleDesc) o;
        }catch (ClassCastException e){
            return false;
        }
        if(this.getSize() != otd.getSize())
            return false;
        for(int i =0; i<this.numFields; i++){
            if(this.getFieldType(i) != otd.getFieldType(i))
                return false;
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        String tupledescriptorstr = new String();
        for (int i=0; i<this.numFields; i++){
            tupledescriptorstr += fieldTypes[i];
            tupledescriptorstr += "(";
            tupledescriptorstr += fieldNames[i];
            tupledescriptorstr += "),";
        }
        return tupledescriptorstr;
    }
}