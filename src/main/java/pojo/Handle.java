package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Handle extends LoggingPojo {

	private Employee employee;	
	private Order order;	

	//Empty constructor
	public Handle() {}
	
	//Role constructor
	public Handle(Employee employee,Order order){
		this.employee=employee;
		this.order=order;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Handle cloned = (Handle) super.clone();
		cloned.setEmployee((Employee)cloned.getEmployee().clone());	
		cloned.setOrder((Order)cloned.getOrder().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "handle : "+
				"roles : {" +  "employee:Employee={"+employee+"},"+ 
					 "order:Order={"+order+"}"+
				 "}"+
				"";
	}
	public Employee getEmployee() {
		return employee;
	}	

	public void setEmployee(Employee employee) {
		this.employee = employee;
	}
	
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	

}
