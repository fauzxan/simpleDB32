package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    PageId pid;
    Integer tupleno;
    List<Object> recordId;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // Lab-1 Exercise 4
        this.pid = pid;
        this.tupleno = tupleno;

        this.recordId = new ArrayList<Object> (2);
        recordId.add(pid);
        recordId.add(tupleno);
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() throws NoSuchElementException{
        // Lab-1 Exercise 4
        if (this.tupleno != null) {
            return (int) this.recordId.get(1);
        }
        else throw new NoSuchElementException("No such tupleno | RecordId.java | getTupleNumber()");
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId(){
        // Lab-1 Exercise 4
        return (PageId) this.recordId.get(0);
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // Lab-1 Exercise 4
        return this.hashCode() == o.hashCode();
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // Lab-1 Exercise 4
        String hashitem = this.recordId.get(0).toString() + "," + this.recordId.get(1).toString();
        return hashitem.hashCode();
    }

}
