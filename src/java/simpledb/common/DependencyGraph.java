package simpledb.common;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;
import java.util.logging.FileHandler;

import simpledb.transaction.TransactionId;
import simpledb.storage.PageId;
import simpledb.common.LogStuff;






public class DependencyGraph{
    private ConcurrentHashMap<TransactionId, ArrayList<TransactionId>> adjacencyList;
    private LogStuff logger;

    public DependencyGraph(){
        this.adjacencyList = new ConcurrentHashMap<TransactionId, ArrayList<TransactionId>>();
        try{
            this.logger = new LogStuff("DependencyGraph");
        }catch(Exception e){}
    }

    private void insert(TransactionId source, TransactionId destination){
        if (!this.adjacencyList.containsKey(source)){
            this.adjacencyList.put(source, new ArrayList<TransactionId>());
        }
        if (!this.adjacencyList.containsKey(destination)){
            this.adjacencyList.put(destination, new ArrayList<TransactionId>());
        }
        this.adjacencyList.get(source).add(destination);
    }

    private void remove(TransactionId node){
        if (!this.adjacencyList.containsKey(node)){
            System.out.println("Trying to remove node that is not in graph! | DependencyGraph, LockManager | remove(node)");
        }
        this.adjacencyList.remove(node);
        for (TransactionId tid: this.adjacencyList.keySet()){
            if (this.adjacencyList.get(tid).contains(node)){
                this.adjacencyList.get(tid).remove(node);
            }
        }
    }

    public boolean containsCycles(ConcurrentHashMap<PageId, List<LockState>> lockStateMap, ConcurrentHashMap<TransactionId, PageId> waitList){// using DFS to implement this

        int flag = 0; // flag, cuz need to clear adjacency list before returning true or false.

        //get list of pages with only one transaction accessing it, and it has READ_WRITE permission.
        ConcurrentHashMap<PageId, TransactionId> singles = new ConcurrentHashMap<PageId, TransactionId>();
        for (PageId pid: lockStateMap.keySet()){

            if (lockStateMap.get(pid).size() == 1 && lockStateMap.get(pid).get(0).getPerm() == Permissions.READ_WRITE){
                singles.put(pid, lockStateMap.get(pid).get(1).getTid());
//                singles
            }
        }

        //create adjacency list with the singles list just created and the waitlist received from above.
        for (TransactionId tid: waitList.keySet()){
            TransactionId source = tid;
            PageId destinationPid = waitList.get(tid);
            if (singles.containsKey(destinationPid)) {
                TransactionId destination = singles.get(waitList.get(tid));
                this.insert(tid, singles.get(waitList.get(tid)));
            }
        }

        HashSet<TransactionId> explored = new HashSet<TransactionId>();

        // Main cycle detection code.
        for (TransactionId tid: this.adjacencyList.keySet()){
            explored.add(tid);
            boolean result = this.DFS(tid, explored);
            if (result == true){
                flag = 1;
                break;
            }
        }
        // once the detection is done, clear the adjacency list
        for (TransactionId tid: waitList.keySet()){
            if (this.adjacencyList.contains(tid)){
                this.remove(tid);
            }
        }

        return (flag == 0? false:true);
    }

    // helper function for above.
    public boolean DFS(TransactionId node, HashSet<TransactionId> explored){
        for (TransactionId tid: this.adjacencyList.get(node)){
            if (explored.contains(tid)){
                return true;
            }
            explored.add(tid);
            if (this.DFS(tid, explored)){
                return true;
            }
        }
        return false;
    }

}