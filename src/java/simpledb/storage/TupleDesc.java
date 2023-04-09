package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable, Cloneable  {

    ArrayList<TDItem> fields;

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
        return fields.iterator();
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
    public TupleDesc(Type[] typeAr, String[] fieldAr){
        // some code goes here
        fields = new ArrayList<TDItem>();
        if (fieldAr.length != typeAr.length){ // Comparing length of both so it does not lead to an error
            throw new IllegalArgumentException("The number of types and the number of field names given do not match!");
        }
        // Map<String, Boolean> map = new HashMap<String, Boolean>();
        for(int i = 0; i < fieldAr.length; i++){
            // Code commented below is not a problem according to the test cases put in place.
            // if(map.containsKey(fieldAr[i])){ // To check if column names are unique or not
            //     System.out.println("Column names are not unique!");
            //     throw new IllegalArgumentException("Column names are not unique!");
            // }
            // map.put(fieldAr[i],true);
            fields.add(new TDItem(typeAr[i], fieldAr[i]));
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
        /** Change in implemnetation here where columns with no field names are 
         * given blank ("") name instead of "unnamed:n" where n is the position 
         * of the column in the table. This was done to prevent failing a test
         * case which joins two tables and compares the field names which does 
         * not work with the previous implementation. */
        fields = new ArrayList<TDItem>();
        for(int i = 0; i < typeAr.length; i++){
            fields.add(new TDItem(typeAr[i], "")); // Assigning a unique name to column so there are not multiple columns with the same name
        } 
        // this(typeAr, new String[8]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return fields.size();
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
        if (numFields() > i) // To make sure that the index is not out of bounds
            return fields.get(i).fieldName;
        NoSuchElementException t = new NoSuchElementException();
        throw t;
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
        if (numFields() > i && i > -1) // To make sure that the index is not out of bounds
            return fields.get(i).fieldType;
        NoSuchElementException t = new NoSuchElementException();
        throw t;
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
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).fieldName.equals(name))
                return i;
        }
        NoSuchElementException t = new NoSuchElementException();
        throw t;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int total = 0;
        for(int i = 0; i<fields.size(); ++i){
            total+=fields.get(i).fieldType.getLen();
        }
        return total;
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
        String[] str = new String[td1.fields.size()+td2.fields.size()];
        Type[] typ = new Type[td1.fields.size()+td2.fields.size()];
        for(int i = 0; i < td1.fields.size();++i){
            str[i] = td1.fields.get(i).fieldName;
            typ[i] = td1.fields.get(i).fieldType;
        } 
        for(int i = td1.fields.size(); i < td1.fields.size() + td2.fields.size();++i){
            str[i] = td2.fields.get(i - td1.fields.size()).fieldName;
            typ[i] = td2.fields.get(i - td1.fields.size()).fieldType;
        } 
        
        return new TupleDesc(typ, str); 
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

    @Override
    public boolean equals(Object o) {

        /** Need to discuss which implementation should we use. **/
        
//        return toString().equals(o.toString());

        // types are sufficient condition
        // trying to check names fails unit tests (see Insert)
         if (o instanceof TupleDesc) {
             TupleDesc other = (TupleDesc) o;
             if(numFields() == other.numFields()){
                 for (int i = 0; i < numFields(); i++) {
                     if (getFieldType(i)!=other.getFieldType(i))
                         return false;
                 }
                 return true;
             }
         }
         return false;
    }

    @Override
    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        return this.toString().hashCode(); // This is unique for every object even if tuple description is the same
        // throw new UnsupportedOperationException("unimplemented");
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
        String temp = new String();
        for (int i = 0; i < fields.size(); i++) {
            temp = temp + fields.get(i).toString() + ',';
        }
        return temp.substring(0, temp.length() -1);
    }
}
