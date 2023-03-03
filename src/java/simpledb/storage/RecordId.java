package simpledb.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.NoSuchElementException;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;
    PageId pid;
    Integer tupleno;

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
        // some code goes here
        this.pid = pid;
        this.tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() throws NoSuchElementException{
        // some code goes here
        if (this.tupleno != null) {
            return this.tupleno;
        }
        else throw new NoSuchElementException("No such tupleno");
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId(){
        // some code goes here
            return this.pid;

    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        return this.hashCode() == o.hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        String hashitem = this.tupleno.toString() + "," + this.pid.toString();
        return hashitem.hashCode();

        // throw new UnsupportedOperationException("implement this");

    }

}
