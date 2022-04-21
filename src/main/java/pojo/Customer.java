package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Customer extends LoggingPojo {

	private String id;
	private String city;
	private String companyName;
	private String contactName;
	private String contactTitle;
	private String country;
	private String fax;
	private String phone;
	private String postalCode;
	private String region;
	private String address;

	public enum make_by {
		client
	}
	private Order order;

	// Empty constructor
	public Customer() {}

	// Constructor on Identifier
	public Customer(String id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Customer(String id,String city,String companyName,String contactName,String contactTitle,String country,String fax,String phone,String postalCode,String region,String address) {
		this.id = id;
		this.city = city;
		this.companyName = companyName;
		this.contactName = contactName;
		this.contactTitle = contactTitle;
		this.country = country;
		this.fax = fax;
		this.phone = phone;
		this.postalCode = postalCode;
		this.region = region;
		this.address = address;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Customer Customer = (Customer) o;
		boolean eqSimpleAttr = Objects.equals(id,Customer.id) && Objects.equals(city,Customer.city) && Objects.equals(companyName,Customer.companyName) && Objects.equals(contactName,Customer.contactName) && Objects.equals(contactTitle,Customer.contactTitle) && Objects.equals(country,Customer.country) && Objects.equals(fax,Customer.fax) && Objects.equals(phone,Customer.phone) && Objects.equals(postalCode,Customer.postalCode) && Objects.equals(region,Customer.region) && Objects.equals(address,Customer.address);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(order, Customer.order) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Customer { " + "id="+id +", "+
					"city="+city +", "+
					"companyName="+companyName +", "+
					"contactName="+contactName +", "+
					"contactTitle="+contactTitle +", "+
					"country="+country +", "+
					"fax="+fax +", "+
					"phone="+phone +", "+
					"postalCode="+postalCode +", "+
					"region="+region +", "+
					"address="+address +"}"; 
	}
	
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public String getContactName() {
		return contactName;
	}

	public void setContactName(String contactName) {
		this.contactName = contactName;
	}
	public String getContactTitle() {
		return contactTitle;
	}

	public void setContactTitle(String contactTitle) {
		this.contactTitle = contactTitle;
	}
	public String getCountry() {
		return country;
	}

	public void setCountry(String country) {
		this.country = country;
	}
	public String getFax() {
		return fax;
	}

	public void setFax(String fax) {
		this.fax = fax;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
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
	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	

	public Order _getOrder() {
		return order;
	}

	public void _setOrder(Order order) {
		this.order = order;
	}
}
