package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Supplier extends LoggingPojo {

	private Integer id;
	private String address;
	private String city;
	private String companyName;
	private String contactName;
	private String contactTitle;
	private String country;
	private String fax;
	private String homePage;
	private String phone;
	private String postalCode;
	private String region;

	public enum insert {
		supplier
	}
	private List<Product> productList;

	// Empty constructor
	public Supplier() {}

	// Constructor on Identifier
	public Supplier(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Supplier(Integer id,String address,String city,String companyName,String contactName,String contactTitle,String country,String fax,String homePage,String phone,String postalCode,String region) {
		this.id = id;
		this.address = address;
		this.city = city;
		this.companyName = companyName;
		this.contactName = contactName;
		this.contactTitle = contactTitle;
		this.country = country;
		this.fax = fax;
		this.homePage = homePage;
		this.phone = phone;
		this.postalCode = postalCode;
		this.region = region;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Supplier Supplier = (Supplier) o;
		boolean eqSimpleAttr = Objects.equals(id,Supplier.id) && Objects.equals(address,Supplier.address) && Objects.equals(city,Supplier.city) && Objects.equals(companyName,Supplier.companyName) && Objects.equals(contactName,Supplier.contactName) && Objects.equals(contactTitle,Supplier.contactTitle) && Objects.equals(country,Supplier.country) && Objects.equals(fax,Supplier.fax) && Objects.equals(homePage,Supplier.homePage) && Objects.equals(phone,Supplier.phone) && Objects.equals(postalCode,Supplier.postalCode) && Objects.equals(region,Supplier.region);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(productList, Supplier.productList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Supplier { " + "id="+id +", "+
					"address="+address +", "+
					"city="+city +", "+
					"companyName="+companyName +", "+
					"contactName="+contactName +", "+
					"contactTitle="+contactTitle +", "+
					"country="+country +", "+
					"fax="+fax +", "+
					"homePage="+homePage +", "+
					"phone="+phone +", "+
					"postalCode="+postalCode +", "+
					"region="+region +"}"; 
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
	public String getHomePage() {
		return homePage;
	}

	public void setHomePage(String homePage) {
		this.homePage = homePage;
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

	

	public List<Product> _getProductList() {
		return productList;
	}

	public void _setProductList(List<Product> productList) {
		this.productList = productList;
	}
}
