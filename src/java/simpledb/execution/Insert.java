package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private int tableid;
    private boolean isCalled;
    private TupleDesc outputTD;

    /**
     * Constructor.
     *
     * @param tid
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId tid, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        TupleDesc table_td = Database.getCatalog().getTupleDesc(tableId);
        TupleDesc opit_td = child.getTupleDesc();
        if (!opit_td.equals(table_td))
            throw new DbException("tupledesc mismatch");

        this.tid = tid;
        this.child = child;
        this.tableid = tableId;
        this.isCalled=false;
        this.outputTD = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        //reminder: return the TupleDesc of the output tuples of this operator
        return outputTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        isCalled=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        isCalled=false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (isCalled)
            return null;

        isCalled = true;

        Tuple num_tuple = new Tuple(outputTD);
        int tupleInserted = 0;
        while(child.hasNext()){
            Tuple t = child.next();
            try{
                Database.getBufferPool().insertTuple(tid, tableid, t);
                tupleInserted += 1;
            } catch (IOException e){
                throw new DbException("fail to insert");
            }
        }
        num_tuple.setField(0, new IntField(tupleInserted));
        return num_tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
