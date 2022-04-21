package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Make_by extends LoggingPojo {

	private Order order;	
	private Customer client;	

	//Empty constructor
	public Make_by() {}
	
	//Role constructor
	public Make_by(Order order,Customer client){
		this.order=order;
		this.client=client;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Make_by cloned = (Make_by) super.clone();
		cloned.setOrder((Order)cloned.getOrder().clone());	
		cloned.setClient((Customer)cloned.getClient().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "make_by : "+
				"roles : {" +  "order:Order={"+order+"},"+ 
					 "client:Customer={"+client+"}"+
				 "}"+
				"";
	}
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	
	public Customer getClient() {
		return client;
	}	

	public void setClient(Customer client) {
		this.client = client;
	}
	

}
