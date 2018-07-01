public class Transaction {
    private String name;
    private int timestamp;
    private Status status;

    public enum Status {
        Running,
        Waiting,
        Aborting,
        Committing
    }

    /**
     * Constructor for a transaction. The transaction represents an abstract
     * view of an application program. Transactions make requests to acquire
     * and release locks on tables. These locks can be Shared or Exclusive
     * locks. The Lock Manager will handle the requests.
     * @param name of the transaction
     * @param timestamp that transaction began
     */
    public Transaction(String name, int timestamp) {
         this.name = name;
         this.timestamp = timestamp;
         this.status = Status.Running;
    }

    @Override
    public String toString() {
      return String.format(
          "Transaction(name=%s, timestamp=%d, status=%s)",
          name, timestamp, status);
    }

    public String getName() {
        return this.name;
    }

    public void wake() {this.status = Status.Running;}

    public void sleep() {this.status = Status.Waiting;}

    public  void abort() {this.status = Status.Aborting;}

    public Status getStatus() {
        return this.status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public int getTimestamp() {
        return this.timestamp;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (o instanceof Transaction) {
            Transaction otherTransaction  = (Transaction) o;
            return otherTransaction.name.equals(this.name);
        } else {
            return false;
        }
    }
}
