package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Employee extends LoggingPojo {

	private Integer id;
	private String address;
	private LocalDate birthDate;
	private String city;
	private String country;
	private String extension;
	private String firstname;
	private LocalDate hireDate;
	private String homePhone;
	private String lastname;
	private String photo;
	private String postalCode;
	private String region;
	private Double salary;
	private String title;
	private String notes;
	private String photoPath;
	private String titleOfCourtesy;

	public enum are_in {
		employee
	}
	private List<Territory> territoryList;
	public enum report_to {
		lowerEmployee, higherEmployee
	}
	private Employee higherEmployee;
	private List<Employee> lowerEmployeeList;
	public enum handle {
		employee
	}
	private List<Order> orderList;

	// Empty constructor
	public Employee() {}

	// Constructor on Identifier
	public Employee(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Employee(Integer id,String address,LocalDate birthDate,String city,String country,String extension,String firstname,LocalDate hireDate,String homePhone,String lastname,String photo,String postalCode,String region,Double salary,String title,String notes,String photoPath,String titleOfCourtesy) {
		this.id = id;
		this.address = address;
		this.birthDate = birthDate;
		this.city = city;
		this.country = country;
		this.extension = extension;
		this.firstname = firstname;
		this.hireDate = hireDate;
		this.homePhone = homePhone;
		this.lastname = lastname;
		this.photo = photo;
		this.postalCode = postalCode;
		this.region = region;
		this.salary = salary;
		this.title = title;
		this.notes = notes;
		this.photoPath = photoPath;
		this.titleOfCourtesy = titleOfCourtesy;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Employee Employee = (Employee) o;
		boolean eqSimpleAttr = Objects.equals(id,Employee.id) && Objects.equals(address,Employee.address) && Objects.equals(birthDate,Employee.birthDate) && Objects.equals(city,Employee.city) && Objects.equals(country,Employee.country) && Objects.equals(extension,Employee.extension) && Objects.equals(firstname,Employee.firstname) && Objects.equals(hireDate,Employee.hireDate) && Objects.equals(homePhone,Employee.homePhone) && Objects.equals(lastname,Employee.lastname) && Objects.equals(photo,Employee.photo) && Objects.equals(postalCode,Employee.postalCode) && Objects.equals(region,Employee.region) && Objects.equals(salary,Employee.salary) && Objects.equals(title,Employee.title) && Objects.equals(notes,Employee.notes) && Objects.equals(photoPath,Employee.photoPath) && Objects.equals(titleOfCourtesy,Employee.titleOfCourtesy);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(territoryList, Employee.territoryList) &&
	Objects.equals(higherEmployee, Employee.higherEmployee) &&
	Objects.equals(lowerEmployeeList, Employee.lowerEmployeeList) &&
	Objects.equals(orderList, Employee.orderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Employee { " + "id="+id +", "+
					"address="+address +", "+
					"birthDate="+birthDate +", "+
					"city="+city +", "+
					"country="+country +", "+
					"extension="+extension +", "+
					"firstname="+firstname +", "+
					"hireDate="+hireDate +", "+
					"homePhone="+homePhone +", "+
					"lastname="+lastname +", "+
					"photo="+photo +", "+
					"postalCode="+postalCode +", "+
					"region="+region +", "+
					"salary="+salary +", "+
					"title="+title +", "+
					"notes="+notes +", "+
					"photoPath="+photoPath +", "+
					"titleOfCourtesy="+titleOfCourtesy +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}
	public LocalDate getBirthDate() {
		return birthDate;
	}

	public void setBirthDate(LocalDate birthDate) {
		this.birthDate = birthDate;
	}
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
	public String getExtension() {
		return extension;
	}

	public void setExtension(String extension) {
		this.extension = extension;
	}
	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}
	public LocalDate getHireDate() {
		return hireDate;
	}

	public void setHireDate(LocalDate hireDate) {
		this.hireDate = hireDate;
	}
	public String getHomePhone() {
		return homePhone;
	}

	public void setHomePhone(String homePhone) {
		this.homePhone = homePhone;
	}
	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}
	public String getPhoto() {
		return photo;
	}

	public void setPhoto(String photo) {
		this.photo = photo;
	}
	public String getPostalCode() {
		return postalCode;
	}

	public void setPostalCode(String postalCode) {
		this.postalCode = postalCode;
	}
	public String getRegion() {
		return region;
	}

	public void setRegion(String region) {
		this.region = region;
	}
	public Double getSalary() {
		return salary;
	}

	public void setSalary(Double salary) {
		this.salary = salary;
	}
	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}
	public String getNotes() {
		return notes;
	}

	public void setNotes(String notes) {
		this.notes = notes;
	}
	public String getPhotoPath() {
		return photoPath;
	}

	public void setPhotoPath(String photoPath) {
		this.photoPath = photoPath;
	}
	public String getTitleOfCourtesy() {
		return titleOfCourtesy;
	}

	public void setTitleOfCourtesy(String titleOfCourtesy) {
		this.titleOfCourtesy = titleOfCourtesy;
	}

	

	public List<Territory> _getTerritoryList() {
		return territoryList;
	}

	public void _setTerritoryList(List<Territory> territoryList) {
		this.territoryList = territoryList;
	}
	public Employee _getHigherEmployee() {
		return higherEmployee;
	}

	public void _setHigherEmployee(Employee higherEmployee) {
		this.higherEmployee = higherEmployee;
	}
	public List<Employee> _getLowerEmployeeList() {
		return lowerEmployeeList;
	}

	public void _setLowerEmployeeList(List<Employee> lowerEmployeeList) {
		this.lowerEmployeeList = lowerEmployeeList;
	}
	public List<Order> _getOrderList() {
		return orderList;
	}

	public void _setOrderList(List<Order> orderList) {
		this.orderList = orderList;
	}
}
