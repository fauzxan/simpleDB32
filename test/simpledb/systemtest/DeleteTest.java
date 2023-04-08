package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import simpledb.common.DbException;
import simpledb.execution.Delete;
import simpledb.execution.Filter;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.storage.DbFileIterator;

public class DeleteTest extends FilterBase {
    List<List<Integer>> expectedTuples = null;

    @Override
    protected int applyPredicate(HeapFile table, TransactionId tid, Predicate predicate)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Filter filter = new Filter(predicate, ss);
        Delete deleteOperator = new Delete(tid, filter);
//        Query q = new Query(deleteOperator, tid);

//        q.start();


        deleteOperator.open();
        boolean hasResult = false;
        int result = -1;
        while (deleteOperator.hasNext()) {
            Tuple t = deleteOperator.next();//eveything gets deleted

            assertFalse(hasResult);
            hasResult = true;
            assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, t.getTupleDesc());
            result = ((IntField) t.getField(0)).getValue();
            System.out.println("\nDeleting: ");
            System.out.println(t.toString());
        }
        assertTrue(hasResult);

        deleteOperator.close();
        System.out.println("\nresult");
        System.out.println(result);
        // As part of the same transaction, scan the table
        if (result == 0) {
            // Deleted zero tuples: all tuples still in table
            expectedTuples = createdTuples;
        } else {
            assert result == createdTuples.size();
            expectedTuples = new ArrayList<>();
        }

        System.out.println("\n\nBEFORE MATCH TUPLE");
        DbFileIterator tt = table.iterator(tid);
        tt.open();
        while (tt.hasNext()){
            System.out.println(tt.next());
        }
        tt.close();

        SystemTestUtil.matchTuples(table, tid, expectedTuples);
        return result;
    }

    @Override
    protected void validateAfter(HeapFile table)
            throws DbException, TransactionAbortedException, IOException {
        // As part of a different transaction, scan the table
        SystemTestUtil.matchTuples(table, expectedTuples);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(DeleteTest.class);
    }
}
