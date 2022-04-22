package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Territory extends LoggingPojo {

	private Integer id;
	private String description;

	public enum contains {
		territory
	}
	private Region region;
	public enum are_in {
		territory
	}
	private List<Employee> employeeList;

	// Empty constructor
	public Territory() {}

	// Constructor on Identifier
	public Territory(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Territory(Integer id,String description) {
		this.id = id;
		this.description = description;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Territory Territory = (Territory) o;
		boolean eqSimpleAttr = Objects.equals(id,Territory.id) && Objects.equals(description,Territory.description);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(region, Territory.region) &&
	Objects.equals(employeeList, Territory.employeeList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Territory { " + "id="+id +", "+
					"description="+description +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	

	public Region _getRegion() {
		return region;
	}

	public void _setRegion(Region region) {
		this.region = region;
	}
	public List<Employee> _getEmployeeList() {
		return employeeList;
	}

	public void _setEmployeeList(List<Employee> employeeList) {
		this.employeeList = employeeList;
	}
}
