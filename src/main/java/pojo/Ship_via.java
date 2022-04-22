package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Ship_via extends LoggingPojo {

	private Shipper shipper;	
	private Order order;	

	//Empty constructor
	public Ship_via() {}
	
	//Role constructor
	public Ship_via(Shipper shipper,Order order){
		this.shipper=shipper;
		this.order=order;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Ship_via cloned = (Ship_via) super.clone();
		cloned.setShipper((Shipper)cloned.getShipper().clone());	
		cloned.setOrder((Order)cloned.getOrder().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "ship_via : "+
				"roles : {" +  "shipper:Shipper={"+shipper+"},"+ 
					 "order:Order={"+order+"}"+
				 "}"+
				"";
	}
	public Shipper getShipper() {
		return shipper;
	}	

	public void setShipper(Shipper shipper) {
		this.shipper = shipper;
	}
	
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	

}
