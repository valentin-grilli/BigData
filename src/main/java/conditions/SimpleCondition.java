package conditions;

import pojo.*;

public class SimpleCondition<E> extends Condition<E> {

	private E attribute;
	private Operator operator;
	private Object value;

	public SimpleCondition(E attribute, Operator operator, Object value) {
		setAttribute(attribute);
		setOperator(operator);
		setValue(value);
	}

	public E getAttribute() {
		return this.attribute;
	}

	public void setAttribute(E attribute) {
		this.attribute = attribute;
	}

	public Operator getOperator() {
		return this.operator;
	}

	public void setOperator(Operator operator) {
		this.operator = operator;
	}

	public Object getValue() {
		return this.value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	@Override
	public boolean hasOrCondition() {
		return false;
	}

	@Override
	public Class<E> eval() throws Exception {
		if(getOperator() == null)
			throw new Exception("You cannot specify a NULL operator in a simple condition");
		if(getValue() == null && operator != Operator.EQUALS && operator != Operator.NOT_EQUALS)
			throw new Exception("You cannot specify a NULL value with this operator");

		return (Class<E>) attribute.getClass();
	}

	@Override
	public boolean evaluate(IPojo obj) {
		if(obj instanceof Shipper)
			return evaluateShipper((Shipper) obj);
		if(obj instanceof Product)
			return evaluateProduct((Product) obj);
		if(obj instanceof Supplier)
			return evaluateSupplier((Supplier) obj);
		if(obj instanceof Customer)
			return evaluateCustomer((Customer) obj);
		if(obj instanceof Order)
			return evaluateOrder((Order) obj);
		if(obj instanceof Category)
			return evaluateCategory((Category) obj);
		if(obj instanceof Employee)
			return evaluateEmployee((Employee) obj);
		if(obj instanceof Region)
			return evaluateRegion((Region) obj);
		if(obj instanceof Territory)
			return evaluateTerritory((Territory) obj);
		if(obj instanceof Composed_of)
			return evaluateComposed_of((Composed_of) obj);
		return true;
	}


	private boolean evaluateShipper(Shipper obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ShipperAttribute attr = (ShipperAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ShipperAttribute.id)
			objectValue = obj.getId();
		if(attr == ShipperAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == ShipperAttribute.phone)
			objectValue = obj.getPhone();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateProduct(Product obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ProductAttribute attr = (ProductAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ProductAttribute.id)
			objectValue = obj.getId();
		if(attr == ProductAttribute.name)
			objectValue = obj.getName();
		if(attr == ProductAttribute.supplierRef)
			objectValue = obj.getSupplierRef();
		if(attr == ProductAttribute.categoryRef)
			objectValue = obj.getCategoryRef();
		if(attr == ProductAttribute.quantityPerUnit)
			objectValue = obj.getQuantityPerUnit();
		if(attr == ProductAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ProductAttribute.reorderLevel)
			objectValue = obj.getReorderLevel();
		if(attr == ProductAttribute.discontinued)
			objectValue = obj.getDiscontinued();
		if(attr == ProductAttribute.unitsInStock)
			objectValue = obj.getUnitsInStock();
		if(attr == ProductAttribute.unitsOnOrder)
			objectValue = obj.getUnitsOnOrder();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateSupplier(Supplier obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		SupplierAttribute attr = (SupplierAttribute) this.attribute;
		Object objectValue = null;

		if(attr == SupplierAttribute.id)
			objectValue = obj.getId();
		if(attr == SupplierAttribute.address)
			objectValue = obj.getAddress();
		if(attr == SupplierAttribute.city)
			objectValue = obj.getCity();
		if(attr == SupplierAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == SupplierAttribute.contactName)
			objectValue = obj.getContactName();
		if(attr == SupplierAttribute.contactTitle)
			objectValue = obj.getContactTitle();
		if(attr == SupplierAttribute.country)
			objectValue = obj.getCountry();
		if(attr == SupplierAttribute.fax)
			objectValue = obj.getFax();
		if(attr == SupplierAttribute.homePage)
			objectValue = obj.getHomePage();
		if(attr == SupplierAttribute.phone)
			objectValue = obj.getPhone();
		if(attr == SupplierAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == SupplierAttribute.region)
			objectValue = obj.getRegion();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateCustomer(Customer obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		CustomerAttribute attr = (CustomerAttribute) this.attribute;
		Object objectValue = null;

		if(attr == CustomerAttribute.id)
			objectValue = obj.getId();
		if(attr == CustomerAttribute.city)
			objectValue = obj.getCity();
		if(attr == CustomerAttribute.companyName)
			objectValue = obj.getCompanyName();
		if(attr == CustomerAttribute.contactName)
			objectValue = obj.getContactName();
		if(attr == CustomerAttribute.contactTitle)
			objectValue = obj.getContactTitle();
		if(attr == CustomerAttribute.country)
			objectValue = obj.getCountry();
		if(attr == CustomerAttribute.fax)
			objectValue = obj.getFax();
		if(attr == CustomerAttribute.phone)
			objectValue = obj.getPhone();
		if(attr == CustomerAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == CustomerAttribute.region)
			objectValue = obj.getRegion();
		if(attr == CustomerAttribute.address)
			objectValue = obj.getAddress();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateOrder(Order obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		OrderAttribute attr = (OrderAttribute) this.attribute;
		Object objectValue = null;

		if(attr == OrderAttribute.id)
			objectValue = obj.getId();
		if(attr == OrderAttribute.freight)
			objectValue = obj.getFreight();
		if(attr == OrderAttribute.orderDate)
			objectValue = obj.getOrderDate();
		if(attr == OrderAttribute.requiredDate)
			objectValue = obj.getRequiredDate();
		if(attr == OrderAttribute.shipAddress)
			objectValue = obj.getShipAddress();
		if(attr == OrderAttribute.shipCity)
			objectValue = obj.getShipCity();
		if(attr == OrderAttribute.shipCountry)
			objectValue = obj.getShipCountry();
		if(attr == OrderAttribute.shipName)
			objectValue = obj.getShipName();
		if(attr == OrderAttribute.shipPostalCode)
			objectValue = obj.getShipPostalCode();
		if(attr == OrderAttribute.shipRegion)
			objectValue = obj.getShipRegion();
		if(attr == OrderAttribute.shippedDate)
			objectValue = obj.getShippedDate();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateCategory(Category obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		CategoryAttribute attr = (CategoryAttribute) this.attribute;
		Object objectValue = null;

		if(attr == CategoryAttribute.id)
			objectValue = obj.getId();
		if(attr == CategoryAttribute.categoryName)
			objectValue = obj.getCategoryName();
		if(attr == CategoryAttribute.description)
			objectValue = obj.getDescription();
		if(attr == CategoryAttribute.picture)
			objectValue = obj.getPicture();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateEmployee(Employee obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		EmployeeAttribute attr = (EmployeeAttribute) this.attribute;
		Object objectValue = null;

		if(attr == EmployeeAttribute.id)
			objectValue = obj.getId();
		if(attr == EmployeeAttribute.address)
			objectValue = obj.getAddress();
		if(attr == EmployeeAttribute.birthDate)
			objectValue = obj.getBirthDate();
		if(attr == EmployeeAttribute.city)
			objectValue = obj.getCity();
		if(attr == EmployeeAttribute.country)
			objectValue = obj.getCountry();
		if(attr == EmployeeAttribute.extension)
			objectValue = obj.getExtension();
		if(attr == EmployeeAttribute.firstname)
			objectValue = obj.getFirstname();
		if(attr == EmployeeAttribute.hireDate)
			objectValue = obj.getHireDate();
		if(attr == EmployeeAttribute.homePhone)
			objectValue = obj.getHomePhone();
		if(attr == EmployeeAttribute.lastname)
			objectValue = obj.getLastname();
		if(attr == EmployeeAttribute.photo)
			objectValue = obj.getPhoto();
		if(attr == EmployeeAttribute.postalCode)
			objectValue = obj.getPostalCode();
		if(attr == EmployeeAttribute.region)
			objectValue = obj.getRegion();
		if(attr == EmployeeAttribute.salary)
			objectValue = obj.getSalary();
		if(attr == EmployeeAttribute.title)
			objectValue = obj.getTitle();
		if(attr == EmployeeAttribute.notes)
			objectValue = obj.getNotes();
		if(attr == EmployeeAttribute.photoPath)
			objectValue = obj.getPhotoPath();
		if(attr == EmployeeAttribute.titleOfCourtesy)
			objectValue = obj.getTitleOfCourtesy();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateRegion(Region obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		RegionAttribute attr = (RegionAttribute) this.attribute;
		Object objectValue = null;

		if(attr == RegionAttribute.id)
			objectValue = obj.getId();
		if(attr == RegionAttribute.description)
			objectValue = obj.getDescription();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateTerritory(Territory obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		TerritoryAttribute attr = (TerritoryAttribute) this.attribute;
		Object objectValue = null;

		if(attr == TerritoryAttribute.id)
			objectValue = obj.getId();
		if(attr == TerritoryAttribute.description)
			objectValue = obj.getDescription();

		return operator.evaluate(objectValue, this.getValue());
	}
		private boolean evaluateComposed_of(Composed_of obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		Composed_ofAttribute attr = (Composed_ofAttribute) this.attribute;
		Object objectValue = null;

		if(attr == Composed_ofAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == Composed_ofAttribute.quantity)
			objectValue = obj.getQuantity();
		if(attr == Composed_ofAttribute.discount)
			objectValue = obj.getDiscount();

		return operator.evaluate(objectValue, this.getValue());
	}

	
}
