package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Shipper extends LoggingPojo {

	private Integer id;
	private String companyName;
	private String phone;

	public enum ship_via {
		shipper
	}
	private List<Order> orderList;

	// Empty constructor
	public Shipper() {}

	// Constructor on Identifier
	public Shipper(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Shipper(Integer id,String companyName,String phone) {
		this.id = id;
		this.companyName = companyName;
		this.phone = phone;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Shipper Shipper = (Shipper) o;
		boolean eqSimpleAttr = Objects.equals(id,Shipper.id) && Objects.equals(companyName,Shipper.companyName) && Objects.equals(phone,Shipper.phone);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(orderList, Shipper.orderList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Shipper { " + "id="+id +", "+
					"companyName="+companyName +", "+
					"phone="+phone +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getCompanyName() {
		return companyName;
	}

	public void setCompanyName(String companyName) {
		this.companyName = companyName;
	}
	public String getPhone() {
		return phone;
	}

	public void setPhone(String phone) {
		this.phone = phone;
	}

	

	public List<Order> _getOrderList() {
		return orderList;
	}

	public void _setOrderList(List<Order> orderList) {
		this.orderList = orderList;
	}
}
