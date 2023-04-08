//package simpledb.execution;
//
//import simpledb.common.Database;
//import simpledb.common.DbException;
//import simpledb.common.Type;
//import simpledb.storage.BufferPool;
//import simpledb.storage.IntField;
//import simpledb.storage.Tuple;
//import simpledb.storage.TupleDesc;
//import simpledb.transaction.TransactionAbortedException;
//import simpledb.transaction.TransactionId;
//
//import java.io.IOException;
//
///**
// * The delete operator. Delete reads tuples from its child operator and removes
// * them from the table they belong to.
// */
//public class Delete extends Operator {
//
//    private static final long serialVersionUID = 1L;
//
//    private TransactionId tid;
//
//    private OpIterator child;
//
//    private TupleDesc td;
//
//    private boolean hasEntered;
//
//    private int count;
//
//    /**
//     * Constructor specifying the transaction that this delete belongs to as
//     * well as the child to read from.
//     *
//     * @param t
//     *            The transaction this delete runs in
//     * @param child
//     *            The child operator from which to read tuples for deletion
//     */
//    public Delete(TransactionId t, OpIterator child) {
//        // some code goes here
//        this.tid = t;
//        this.child = child;
//        this.count = -1;
//        Type[] typeAr = new Type[1];
//        typeAr[0] = Type.INT_TYPE;
//        String[] stringAr = new String[1];
//        stringAr[0] = null;
//        td = new TupleDesc(typeAr, stringAr);
//        //td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{null});
//    }
//
//    public TupleDesc getTupleDesc() {
//        // some code goes here
//        return this.td;
//    }
//
//    public void open() throws DbException, TransactionAbortedException {
//        // some code goes here
//        this.child.open();
//        super.open();
//        this.count =0;
//        hasEntered = false;
//        while (child.hasNext()) {
//            Tuple next = child.next();
//            Database.getBufferPool().deleteTuple(tid, next);
//            count++;
//        }
//    }
//
//    public void close() {
//        // some code goes here
//        super.close();
//        this.count = -1;
//        child.close();
//    }
//
//    public void rewind() throws DbException, TransactionAbortedException {
//        // some code goes here
//        hasEntered = false;
//        this.child.rewind();
//        this.count = 0;
//    }
//
//    /**
//     * Deletes tuples as they are read from the child operator. Deletes are
//     * processed via the buffer pool (which can be accessed via the
//     * Database.getBufferPool() method.
//     *
//     * @return A 1-field tuple containing the number of deleted records.
//     * @see Database#getBufferPool
//     * @see BufferPool#deleteTuple
//     */
//    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
//        // some code goes here
//        if (hasEntered) {
//            return null;
//        }
//        hasEntered = true;
//        Tuple deleted_num=new Tuple(getTupleDesc());
//        deleted_num.setField(0,new IntField(this.count));
//        return deleted_num;
//    }
//
//    @Override
//    public OpIterator[] getChildren() {
//        // some code goes here
//        return new OpIterator[]{this.child};
//    }
//
//    @Override
//    public void setChildren(OpIterator[] children) {
//        // some code goes here
//        this.child = children[0];
//    }
//
//}











package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private OpIterator child;
    private boolean isCalled;
    private TupleDesc outputTD;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param tid
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId tid, OpIterator child) {
        // some code goes here
        this.tid = tid;
        this.child = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //TO CHECK >> hasn't been mentioned here, but similar to Insert should be called only once per transaction?
        //without isCalled check, tests fail on fetchNext
        if (isCalled)
            return null;

        isCalled = true;

        Tuple num_tuple = new Tuple(outputTD);
        int tupleDeleted = 0;
        while(child.hasNext()){
            Tuple t = child.next();
            try{
                Database.getBufferPool().deleteTuple(tid, t);
                tupleDeleted += 1;
            } catch (DbException e){
                throw new DbException("fail to delete");
            }
        }
        num_tuple.setField(0, new IntField(tupleDeleted));
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
