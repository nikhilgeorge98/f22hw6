package paxos;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.*;
import java.time.Instant;

/**
 * This class is the main class you need to implement paxos instances.
 */
public class Paxos implements PaxosRMI, Runnable{

    ReentrantLock mutex;
    String[] peers; // hostname
    int[] ports; // host port
    int me; // index into peers[]

    Registry registry;
    PaxosRMI stub;

    AtomicBoolean dead;// for testing
    AtomicBoolean unreliable;// for testing

    // Your data here
    int seq;
    Object value;
    // Map<Integer, Object> seq2Obj = new HashMap<>();
    // Map<Integer, PaxosInstance> seq2paxIns = new HashMap<>();
//    Map<Integer, Object> seq2Obj = new ConcurrentHashMap<>();
    Map<Integer, PaxosInstance> seq2paxIns = new ConcurrentHashMap<>();

    List<Integer> done = new ArrayList<>();

    /**
     * Call the constructor to create a Paxos peer.
     * The hostnames of all the Paxos peers (including this one)
     * are in peers[]. The ports are in ports[].
     */
    public Paxos(int me, String[] peers, int[] ports){

        this.me = me;
        this.peers = peers;
        this.ports = ports;
        this.mutex = new ReentrantLock();
        this.dead = new AtomicBoolean(false);
        this.unreliable = new AtomicBoolean(false);

        // Your initialization code here
        this.seq = -1;
        this.value = null;

        for (int i = 1; i <= this.peers.length; i++) {
            this.done.add(-1);
        }

        // register peers, do not modify this part
        try{
            System.setProperty("java.rmi.server.hostname", this.peers[this.me]);
            registry = LocateRegistry.createRegistry(this.ports[this.me]);
            stub = (PaxosRMI) UnicastRemoteObject.exportObject(this, this.ports[this.me]);
            registry.rebind("Paxos", stub);
        } catch(Exception e){
            e.printStackTrace();
        }
    }


    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a response message if
     * the server responded, and return null if Call() was not
     * be able to contact the server.
     *
     * You should assume that Call() will time out and return
     * null after a while if it doesn't get a reply from the server.
     *
     * Please use Call() to send all RMIs and please don't change
     * this function.
     */
    public Response Call(String rmi, Request req, int id){
        Response callReply = null;

        PaxosRMI stub;
        try{
            Registry registry=LocateRegistry.getRegistry(this.ports[id]);
            stub=(PaxosRMI) registry.lookup("Paxos");
            if(rmi.equals("Prepare")) {
//                System.out.println("Calllllllllllllllllllllllllllllllllllllllllllll");
                callReply = stub.Prepare(req);
                System.out.println("CaXXXXXXXXXXXXXXXXXxlllllllllllllllllllllllllllllllllllllllllllll");
            }
            else if(rmi.equals("Accept"))
                callReply = stub.Accept(req);
            else if(rmi.equals("Decide"))
                callReply = stub.Decide(req);
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            System.out.println("Exception"+e);
            return null;
        }
        return callReply;
    }


    /**
     * The application wants Paxos to start agreement on instance seq,
     * with proposed value v. Start() should start a new thread to run
     * Paxos on instance seq. Multiple instances can be run concurrently.
     *
     * Hint: You may start a thread using the runnable interface of
     * Paxos object. One Paxos object may have multiple instances, each
     * instance corresponds to one proposed value/command. Java does not
     * support passing arguments to a thread, so you may reset seq and v
     * in Paxos object before starting a new thread. There is one issue
     * that variable may change before the new thread actually reads it.
     * Test won't fail in this case.
     *
     * Start() just starts a new thread to initialize the agreement.
     * The application will call Status() to find out if/when agreement
     * is reached.
     */
    public void Start(int seq, Object value){
//        if(seq < this.Min())
//            return;
        // TODO: update SeqtoObj here.
        // Your code here
        this.seq = seq;
        this.value = value;
//        this.seq2Obj.put(seq, value);
        
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run(){
        //Your code here
        //TODO thread.lock
        if(this.seq < this.Min())
            return;
//        if(this.seq2paxIns.get(this.seq) == null){
//            mutex.lock();
//            this.seq2paxIns.put(this.seq, new PaxosInstance());
////            mutex.unlock();
////            mutex.lock();
//            this.seq2Obj.put(this.seq, this.value);
//            mutex.unlock();
//        }

        this.proposer();

    }

    public void proposer(){
        //need to write
//        int n = this.seq2paxIns.get(this.seq).n_p + this.me;
        int n;

        while(!isDead() && this.getInstance(this.seq).state != State.Decided){// && this.seq2paxIns.get(this.seq).state != State.Decided){
//            n = this.seq2paxIns.get(this.seq).n_p + this.me + (int)Instant.now().toEpochMilli(); //should be unique, this may not work
            n = (int)Instant.now().toEpochMilli();
            System.out.println("proposal number: " + n);
            
            int countPromise = 0;
//            int highest_n_a = -1;
            int highest_n_a = this.seq2paxIns.get(this.seq).n_a;

            Object highest_v_a = new Object();
            
            Request prepare = new Request(this.seq, n, "PREPARE", this.done.get(this.me), this.me);
            Response promiseResp = new Response();
            
            for(int i = 0; i<this.peers.length; i++){
                System.out.println("Prepare" + i + "by" + this.me);
                if(i != this.me) {
                    System.out.println("remote callssssssssssssssssssssssssssssssssssssssssssssss");
                    promiseResp = Call("Prepare", prepare, i);
                }
                else
                    promiseResp = Prepare(prepare);
                System.out.println(promiseResp+"PRRRRRRRRRRRRRRRRRRRRRRRRRRRr");
                if(promiseResp != null && promiseResp.propNo == n){
                    System.out.println("HAPPENING....................................");
//                    this.done.set(i, promiseResp.minDone);
                    System.out.println("promiseResp done " + this.done);
                    countPromise++;
                    if(promiseResp.accepted == "accepted" && promiseResp.acceptedPropNo > highest_n_a){
                        highest_n_a = promiseResp.acceptedPropNo;
                        highest_v_a = promiseResp.value;
                    }
                }

            }
            System.out.println(countPromise+"$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"+this.peers.length+"......."+this.peers.length/2);
            //check majority condition
            if(countPromise>this.peers.length/2){
                Object vprime;
                if(highest_n_a>-1)
                    vprime = highest_v_a;
                else
                    vprime = this.value;

                System.out.println(vprime+"##################################");

                int countAccept = 0;
                
                Request accept_request = new Request(this.seq, vprime, n, "ACCEPT-REQUEST", this.done.get(this.me), this.me);
                Response acceptResp = new Response();

                for(int i = 0; i<this.peers.length; i++){
                    if(i != this.me)
                        acceptResp = Call("Accept", accept_request, i);
                    else
                        acceptResp = Accept(accept_request);
//                    acceptResp = Call("Accept", accept_request, i);

                    if(acceptResp != null){// && acceptResp.propNo == n){
//                        this.done.set(i, acceptResp.minDone);
                        System.out.println("acceptResp done " + this.done);
                        countAccept++;
                    }
                }

                if(countAccept>this.peers.length/2){
                    //broadcast decide
                    System.out.println("DECIDEDddddddddddddddddddddddddddddddddddddddddddddddd");
                    Request decide = new Request(this.seq, vprime, n, "DECIDE", this.done.get(this.me), this.me);
                    Response decideResp;

                    this.done.set(this.me, this.seq);
                    System.out.println("I'm done " + this.done);
                    for(int i = 0; i<this.peers.length; i++){
                        if(i!=me){
                            decideResp = Call("Decide", decide, i);
                            if(decideResp != null) {

                                System.out.println("decideResp done " + this.done + " " + i);
//                                System.out.println("mindone after decide:"+decideResp.minDone);
//                                this.done.set(i, decideResp.minDone);
                                System.out.println("after " + this.done + " " + i);
                            }
                        }
                        else {
                            decideResp = Decide(decide);
                        }
                    }
    
                }

            }            

        }

    }

    // RMI handler
    public Response Prepare(Request req){
        // your code here

        //response format accoring to the google guy - can change to match pseudocode in doc if required
        this.done.set(req.peer, req.minDone);
//        System.out.println(req.propNo +"@@@@@@@ "+ this.seq2paxIns.get(req.seq).n_p);
//        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!"+this.seq2paxIns.get(req.seq));
//        if(req.propNo > this.seq2paxIns.get(req.seq).n_p)
        if(req.propNo > this.getInstance(req.seq).n_p)
        {
//            System.out.println("Before: " + this.seq2paxIns.get(req.seq).n_p+" prop#: "+req.propNo);
            this.seq2paxIns.get(req.seq).n_p = req.propNo;
//            System.out.println("After: " + this.seq2paxIns.get(req.seq).n_p+" prop#: "+req.propNo);
            System.out.println("prepare1");

            if(this.seq2paxIns.get(req.seq).v_a != null) {
                System.out.println("prepare2");
                return (new Response(true, req.propNo, this.seq2paxIns.get(req.seq).v_a, "PROMISE", "accepted", this.seq2paxIns.get(req.seq).n_a));
            }
            else {
                System.out.println("prepare3");
                return (new Response(true, req.propNo, "PROMISE"));
            }
        }
        else
        {
            System.out.println("prepare4");
            return (new Response(false));
        }

    }

    public Response Accept(Request req){
        // your code here

        //response format accoring to the google guy - can change to match pseudocode in doc if required
        this.done.set(req.peer, req.minDone);

//        if(req.propNo >= this.seq2paxIns.get(req.seq).n_p)
        if(req.propNo >= this.getInstance(req.seq).n_p)
        {
            this.seq2paxIns.get(req.seq).n_p = req.propNo;
            this.seq2paxIns.get(req.seq).n_a = req.propNo;
            this.seq2paxIns.get(req.seq).v_a = req.value;

            return (new Response(true, req.propNo, req.value, "ACCEPT"));
        }
        else
        {
            return (new Response(false));
        }

    }

    public Response Decide(Request req){
        // your code here

        this.done.set(req.peer, req.minDone);
        System.out.println(req.peer+"peeeeeeeeeeerrrrrrrrrrrrrrrrrrrrrrrrrrrr" + this.me);

        this.seq2paxIns.get(req.seq).n_p = req.propNo;
        this.seq2paxIns.get(req.seq).n_a = req.propNo;
        this.seq2paxIns.get(req.seq).v_a = req.value;
        this.seq2paxIns.get(req.seq).state = State.Decided;

//        this.Done(req.seq);
        System.out.println(this.done);

        return new Response(true);

    }

    /**
     * The application on this machine is done with
     * all instances <= seq.
     *
     * see the comments for Min() for more explanation.
     */
    public void Done(int seq) {
        // Your code here
        System.out.println("this.meeeeeeeeee" + this.me);
        if(this.done.get(this.me)<=seq)
            this.done.set(this.me, seq);
    }


    /**
     * The application wants to know the
     * highest instance sequence known to
     * this peer.
     */
    public int Max(){
        // Your code here
        int max = -1;
        Iterator<HashMap.Entry<Integer, PaxosInstance>> itr = this.seq2paxIns.entrySet().iterator();
        
        while(itr.hasNext()){
            if(itr.next().getKey()>max)
                max = itr.next().getKey();
        }

        return max;

    }

    /**
     * Min() should return one more than the minimum among z_i,
     * where z_i is the highest number ever passed
     * to Done() on peer i. A peers z_i is -1 if it has
     * never called Done().

     * Paxos is required to have forgotten all information
     * about any instances it knows that are < Min().
     * The point is to free up memory in long-running
     * Paxos-based servers.

     * Paxos peers need to exchange their highest Done()
     * arguments in order to implement Min(). These
     * exchanges can be piggybacked on ordinary Paxos
     * agreement protocol messages, so it is OK if one
     * peers Min does not reflect another Peers Done()
     * until after the next instance is agreed to.

     * The fact that Min() is defined as a minimum over
     * all Paxos peers means that Min() cannot increase until
     * all peers have been heard from. So if a peer is dead
     * or unreachable, other peers Min()s will not increase
     * even if all reachable peers call Done. The reason for
     * this is that when the unreachable peer comes back to
     * life, it will need to catch up on instances that it
     * missed -- the other peers therefore cannot forget these
     * instances.
     */
    public int Min(){
        // Your code here
        int min = Integer.MAX_VALUE;
        for(int seq: this.done){
            if(seq < min)
                min = seq;
        }

        Iterator<HashMap.Entry<Integer, PaxosInstance>> itr = this.seq2paxIns.entrySet().iterator();
        
        while(itr.hasNext()){
            if(itr.next().getKey()<=min){  // && itr.next().getValue().state==State.Decided){
                itr.remove();
//                this.seq2Obj.remove(itr.next().getKey());
            }

        }

        return min + 1;
    }



    /**
     * the application wants to know whether this
     * peer thinks an instance has been decided,
     * and if so what the agreed value is. Status()
     * should just inspect the local peer state;
     * it should not contact other Paxos peers.
     */
    public retStatus Status(int seq){
        // Your code here
        // State s = seq2paxIns.getOrDefault(seq, new PaxosInstance()).state;
        // Object obj = seq2Obj.getOrDefault(seq, null);

        // State s = seq2paxIns.get(seq).state;
        // Object obj = seq2Obj.get(seq);
        System.out.println(this.me+" me @"+seq+"----------------"+this.Min());
        if(seq < this.Min())
            return new retStatus(State.Forgotten, null);
//        if

        if (this.seq2paxIns.containsKey(seq)) {
//            return new retStatus(this.seq2paxIns.get(seq).state, this.seq2Obj.get(seq));
            return new retStatus(this.seq2paxIns.get(seq).state, this.seq2paxIns.get(seq).v_a);
        }
        else
            return new retStatus(State.Pending, null);
    }

    /**
     * helper class for Status() return
     */
    public class retStatus{
        public State state;
        public Object v;

        public retStatus(State state, Object v){
            this.state = state;
            this.v = v;
        }
    }

    public class PaxosInstance{
        int n_p; //highest proposal id seen
        int n_a; //highest accepted id seen
        Object v_a; //value
        State state;

        public PaxosInstance(){
            this.n_a = -1;
            this.n_p = -1;
            this.v_a = null;
            this.state = State.Pending;
        }
    }

    public PaxosInstance getInstance(int seq){
        mutex.lock();
        if(!seq2paxIns.containsKey(seq)){
            PaxosInstance instance = new PaxosInstance();
            seq2paxIns.put(seq, instance);
        }
        mutex.unlock();
        return seq2paxIns.get(seq);
    }

    /**
     * Tell the peer to shut itself down.
     * For testing.
     * Please don't change these four functions.
     */
    public void Kill(){
        this.dead.getAndSet(true);
        if(this.registry != null){
            try {
                UnicastRemoteObject.unexportObject(this.registry, true);
            } catch(Exception e){
                System.out.println("None reference");
            }
        }
    }

    public boolean isDead(){
        return this.dead.get();
    }

    public void setUnreliable(){
        this.unreliable.getAndSet(true);
    }

    public boolean isunreliable(){
        return this.unreliable.get();
    }


}
