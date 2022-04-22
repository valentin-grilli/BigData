package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Are_in extends LoggingPojo {

	private Employee employee;	
	private Territory territory;	

	//Empty constructor
	public Are_in() {}
	
	//Role constructor
	public Are_in(Employee employee,Territory territory){
		this.employee=employee;
		this.territory=territory;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Are_in cloned = (Are_in) super.clone();
		cloned.setEmployee((Employee)cloned.getEmployee().clone());	
		cloned.setTerritory((Territory)cloned.getTerritory().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "are_in : "+
				"roles : {" +  "employee:Employee={"+employee+"},"+ 
					 "territory:Territory={"+territory+"}"+
				 "}"+
				"";
	}
	public Employee getEmployee() {
		return employee;
	}	

	public void setEmployee(Employee employee) {
		this.employee = employee;
	}
	
	public Territory getTerritory() {
		return territory;
	}	

	public void setTerritory(Territory territory) {
		this.territory = territory;
	}
	

}
