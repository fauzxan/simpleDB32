package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.HashMap;


import simpledb.storage.TupleDesc.TDItem;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td;
    private RecordId recordId;
    private int numFields;
    private HashMap<Integer, Field> fieldsList;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     * DONE
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        Iterator<TDItem> iterator = td.iterator();

        if (!iterator.hasNext()){ // if its empty
            System.out.println("Tuple does not have any items");
        }
//        else{
//            while (iterator.hasNext()){
//                TDItem element = iterator.next();
//                if (element.fieldName instanceof String){
//                    continue;
//                }
//                else{
//                    System.out.println("Invalid pair in tuple!");
//                }
//            }
//
//        }
        this.td = td;
        this.recordId = null;
        this.numFields = td.numFields();
        this.fieldsList = new HashMap<Integer, Field>();
        
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     * DONE
     */
    public TupleDesc getTupleDesc() {
        if (this.td == null ){
            return null;
        }
        return this.td;
        // return null;
    }



    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     * DONE
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        if (this.getTupleDesc() != null){
            this.recordId = rid;
        }
        else{
            this.recordId = null;
        }


    }
    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     * DONE
     */
    public RecordId getRecordId() {
        // some code goes here
        if (this.getTupleDesc() == null){
            return null;
        }
        return this.recordId;
    }

    

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here

        if (this.td.getFieldType(i) == f.getType()){
            if (this.fieldsList.containsKey(i)){
                this.fieldsList.replace(i, f);
            }
            else{
                this.fieldsList.put(i, f);
            }
        }
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return this.fieldsList.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        StringBuffer sb = new StringBuffer("");
        for (Field x: this.fieldsList.values()){
            sb.append(x.toString());
            sb.append("\t");
        }
//        throw new UnsupportedOperationException("Implement this");
        return sb.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        if (this.numFields>0){
            return fieldsList.values().iterator();
        }
        return null;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        this.fieldsList.clear();
    }


}
