package simpledb.execution;

import simpledb.common.Database;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    TransactionId tid;
    int tableid;
    String tableAlias;
    DbFileIterator iterator;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // Lab-1 Exercise 6
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
        this.iterator = Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() throws NoSuchElementException{
        // Lab-1 Exercise 6
        String name = Database.getCatalog().getTableName(this.tableid);
        if (name != null) {
            return name;
        }
        else{
            throw new NoSuchElementException("No such table name | SeqScan.java | getTableName()");
        }
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // Lab-1 Exercise 6
        String alias = this.tableAlias;
        if (alias != null){
            return alias;
        }
        else{
            throw new NoSuchElementException("Table Alias not created properly while seqScanning | SeqScan.java | getAlias()");
        }
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // Lab-1 Exercise 6
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // Lab-1 Exercise 6
        this.iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // Lab-1 Exercise 6
        TupleDesc td = Database.getCatalog().getTupleDesc(this.tableid);
        Type[] typeArr = new Type[td.numFields()];
        String[] fieldArr = new String[td.numFields()];
        if (td != null){
            for (int i=0; i<td.numFields(); i++){
                typeArr[i] = td.getFieldType(i);
                fieldArr[i] = td.getFieldName(i);
            }
            return new TupleDesc(typeArr, fieldArr);
        }
        else{
            throw new NoSuchElementException("Could not get tuple desc while seqScanning, or it is empty | SeqScan.java | getTupleDesc()");
        }
    }


    public boolean hasNext() throws TransactionAbortedException, DbException {
        // Lab-1 Exercise 6
        return this.iterator.hasNext();

    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // Lab-1 Exercise 6
        return this.iterator.next();
    }

    public void close() {
        // Lab-1 Exercise 6
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // Lab-1 Exercise 6
        try{
            this.iterator.rewind();
        }
        catch (DbException e1){
            System.out.println("DbException occurred while trying to rewind the connection in SeqScan | SeqScan.java | rewind");
        }
        catch(NoSuchElementException e2){
            System.out.println("NoSuchElementException occurred while trying to rewind the connection in SeqScan | SeqScan.java | rewind");
        }
        catch(TransactionAbortedException e3){
            System.out.println("TransactionAbortedException occurred while trying to rewind the connection in SeqScan | SeqScan.java | rewind");
        }
    }
}
