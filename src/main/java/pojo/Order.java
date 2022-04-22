package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Order extends LoggingPojo {

	private Integer id;
	private Double freight;
	private LocalDate orderDate;
	private LocalDate requiredDate;
	private String shipAddress;
	private String shipCity;
	private String shipCountry;
	private String shipName;
	private String shipPostalCode;
	private String shipRegion;
	private LocalDate shippedDate;

	public enum make_by {
		order
	}
	private Customer client;
	public enum ship_via {
		order
	}
	private Shipper shipper;
	public enum handle {
		order
	}
	private Employee employee;

	// Empty constructor
	public Order() {}

	// Constructor on Identifier
	public Order(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Order(Integer id,Double freight,LocalDate orderDate,LocalDate requiredDate,String shipAddress,String shipCity,String shipCountry,String shipName,String shipPostalCode,String shipRegion,LocalDate shippedDate) {
		this.id = id;
		this.freight = freight;
		this.orderDate = orderDate;
		this.requiredDate = requiredDate;
		this.shipAddress = shipAddress;
		this.shipCity = shipCity;
		this.shipCountry = shipCountry;
		this.shipName = shipName;
		this.shipPostalCode = shipPostalCode;
		this.shipRegion = shipRegion;
		this.shippedDate = shippedDate;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Order Order = (Order) o;
		boolean eqSimpleAttr = Objects.equals(id,Order.id) && Objects.equals(freight,Order.freight) && Objects.equals(orderDate,Order.orderDate) && Objects.equals(requiredDate,Order.requiredDate) && Objects.equals(shipAddress,Order.shipAddress) && Objects.equals(shipCity,Order.shipCity) && Objects.equals(shipCountry,Order.shipCountry) && Objects.equals(shipName,Order.shipName) && Objects.equals(shipPostalCode,Order.shipPostalCode) && Objects.equals(shipRegion,Order.shipRegion) && Objects.equals(shippedDate,Order.shippedDate);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(client, Order.client) &&
	Objects.equals(shipper, Order.shipper) &&
	Objects.equals(employee, Order.employee) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Order { " + "id="+id +", "+
					"freight="+freight +", "+
					"orderDate="+orderDate +", "+
					"requiredDate="+requiredDate +", "+
					"shipAddress="+shipAddress +", "+
					"shipCity="+shipCity +", "+
					"shipCountry="+shipCountry +", "+
					"shipName="+shipName +", "+
					"shipPostalCode="+shipPostalCode +", "+
					"shipRegion="+shipRegion +", "+
					"shippedDate="+shippedDate +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public Double getFreight() {
		return freight;
	}

	public void setFreight(Double freight) {
		this.freight = freight;
	}
	public LocalDate getOrderDate() {
		return orderDate;
	}

	public void setOrderDate(LocalDate orderDate) {
		this.orderDate = orderDate;
	}
	public LocalDate getRequiredDate() {
		return requiredDate;
	}

	public void setRequiredDate(LocalDate requiredDate) {
		this.requiredDate = requiredDate;
	}
	public String getShipAddress() {
		return shipAddress;
	}

	public void setShipAddress(String shipAddress) {
		this.shipAddress = shipAddress;
	}
	public String getShipCity() {
		return shipCity;
	}

	public void setShipCity(String shipCity) {
		this.shipCity = shipCity;
	}
	public String getShipCountry() {
		return shipCountry;
	}

	public void setShipCountry(String shipCountry) {
		this.shipCountry = shipCountry;
	}
	public String getShipName() {
		return shipName;
	}

	public void setShipName(String shipName) {
		this.shipName = shipName;
	}
	public String getShipPostalCode() {
		return shipPostalCode;
	}

	public void setShipPostalCode(String shipPostalCode) {
		this.shipPostalCode = shipPostalCode;
	}
	public String getShipRegion() {
		return shipRegion;
	}

	public void setShipRegion(String shipRegion) {
		this.shipRegion = shipRegion;
	}
	public LocalDate getShippedDate() {
		return shippedDate;
	}

	public void setShippedDate(LocalDate shippedDate) {
		this.shippedDate = shippedDate;
	}

	

	public Customer _getClient() {
		return client;
	}

	public void _setClient(Customer client) {
		this.client = client;
	}
	public Shipper _getShipper() {
		return shipper;
	}

	public void _setShipper(Shipper shipper) {
		this.shipper = shipper;
	}
	public Employee _getEmployee() {
		return employee;
	}

	public void _setEmployee(Employee employee) {
		this.employee = employee;
	}
}
