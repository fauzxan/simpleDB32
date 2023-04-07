package simpledb.common;
import simpledb.transaction.TransactionId;



public class LockState {
    private TransactionId tid;
    private Permissions perm;

    public LockState(TransactionId tid, Permissions perm) {
        this.tid = tid;
        this.perm = perm;
    }

    public TransactionId getTid() {
        return tid;
    }

    public Permissions getPerm() {
        return perm;
    }
}