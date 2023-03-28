package kvpaxos;

import java.util.concurrent.atomic.AtomicInteger;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;


public class Client {
    String[] servers;
    int[] ports;

    // Your data here
    AtomicInteger idHelper = new AtomicInteger(0);
    int clientNo;


    public Client(String[] servers, int[] ports){
        this.servers = servers;
        this.ports = ports;
        // Your initialization code here
        this.clientNo = idHelper.getAndIncrement();
    }

    /**
     * Call() sends an RMI to the RMI handler on server with
     * arguments rmi name, request message, and server id. It
     * waits for the reply and return a res message if
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
        KVPaxosRMI stub;
        try{
            Registry registry= LocateRegistry.getRegistry(this.ports[id]);
            stub=(KVPaxosRMI) registry.lookup("KVPaxos");
            if(rmi.equals("Get"))
                callReply = stub.Get(req);
            else if(rmi.equals("Put")){
                callReply = stub.Put(req);}
            else
                System.out.println("Wrong parameters!");
        } catch(Exception e){
            return null;
        }
        return callReply;
    }

    // RMI handlers
    public Integer Get(String key){
        // Your code here
        Op get_operation = new Op("Get", clientNo, key, null);
    	Request request = new Request(get_operation);
    	Response res = null;
    	int ind = 0;
    	while(res == null) {
            res = Call("Get", request, ind);
    		ind = (ind +1 ) % ports.length;
    	}
    	if(res.operation != null) {
            return res.operation.value;
        }
    	return null;

    }

    public boolean Put(String key, Integer value){
        // Your code here
        Op put_operation = new Op("Put", clientNo, key, value);
    	Request request = new Request(put_operation);
    	Response res = null;
    	int ind = 0;
    	while(res == null) {
    		res = Call("Put", request, ind);
    		ind = (ind + 1) % ports.length;
    	}
    	if(res.operation != null) {
            return true;
        }
        return false;
    }

}
