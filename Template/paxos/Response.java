package paxos;
import java.io.Serializable;

/**
 * Please fill in the data structure you use to represent the response message for each RMI call.
 * Hint: You may need a boolean variable to indicate ack of acceptors and also you may need proposal number and value.
 * Hint: Make it more generic such that you can use it for each RMI call.
 */
public class Response implements Serializable {
    static final long serialVersionUID=2L;
    // your data here

    //is seq required?

    boolean ack; //might need separate for accept and promise
    int propNo;
    Object value;
    String type;
    String accepted;
    int acceptedPropNo;
//    int minDone;

    // Your constructor and methods here
    public Response(){
        this.ack = false; 
        this.propNo = -1;
        this.value = null;
        this.type = "";
        this.accepted = "";
        this.acceptedPropNo = -1;
//        this.minDone = -1;
    }

    //rejects
    public Response(boolean ack){
        this.ack = ack;
//        this.minDone = minDone;
    }

    //TYPE propNo
    public Response(boolean ack, int propNo, String type){
        this.ack = ack;
        this.propNo = propNo;
        this.type = type;
//        this.minDone = minDone;
    }

    //TYPE propNo, value
    public Response(boolean ack, int propNo, Object value, String type){
        this.ack = ack;
        this.propNo = propNo;
        this.value = value;
        this.type = type;
//        this.minDone = minDone;
    }

    //TYPE propNo accepted acceptedPropNo value
    public Response(boolean ack, int propNo, Object value, String type, String accepted, int acceptedPropNo){
        this.ack = ack;
        this.propNo = propNo;
        this.value = value;
        this.type = type;
        this.accepted = accepted;
        this.acceptedPropNo = acceptedPropNo;
//        this.minDone = minDone;
    }

}
