package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Report_to extends LoggingPojo {

	private Employee lowerEmployee;	
	private Employee higherEmployee;	

	//Empty constructor
	public Report_to() {}
	
	//Role constructor
	public Report_to(Employee lowerEmployee,Employee higherEmployee){
		this.lowerEmployee=lowerEmployee;
		this.higherEmployee=higherEmployee;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Report_to cloned = (Report_to) super.clone();
		cloned.setLowerEmployee((Employee)cloned.getLowerEmployee().clone());	
		cloned.setHigherEmployee((Employee)cloned.getHigherEmployee().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "report_to : "+
				"roles : {" +  "lowerEmployee:Employee={"+lowerEmployee+"},"+ 
					 "higherEmployee:Employee={"+higherEmployee+"}"+
				 "}"+
				"";
	}
	public Employee getLowerEmployee() {
		return lowerEmployee;
	}	

	public void setLowerEmployee(Employee lowerEmployee) {
		this.lowerEmployee = lowerEmployee;
	}
	
	public Employee getHigherEmployee() {
		return higherEmployee;
	}	

	public void setHigherEmployee(Employee higherEmployee) {
		this.higherEmployee = higherEmployee;
	}
	

}
