package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Process extends LoggingPojo {

	private Employee employee;	
	private Order order;	

	//Empty constructor
	public Process() {}
	
	//Role constructor
	public Process(Employee employee,Order order){
		this.employee=employee;
		this.order=order;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Process cloned = (Process) super.clone();
		cloned.setEmployee((Employee)cloned.getEmployee().clone());	
		cloned.setOrder((Order)cloned.getOrder().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "process : "+
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
