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
		if(obj instanceof ProductInfo)
			return evaluateProductInfo((ProductInfo) obj);
		if(obj instanceof StockInfo)
			return evaluateStockInfo((StockInfo) obj);
		if(obj instanceof Customer)
			return evaluateCustomer((Customer) obj);
		if(obj instanceof Order)
			return evaluateOrder((Order) obj);
		if(obj instanceof Category)
			return evaluateCategory((Category) obj);
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
	private boolean evaluateProductInfo(ProductInfo obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		ProductInfoAttribute attr = (ProductInfoAttribute) this.attribute;
		Object objectValue = null;

		if(attr == ProductInfoAttribute.id)
			objectValue = obj.getId();
		if(attr == ProductInfoAttribute.name)
			objectValue = obj.getName();
		if(attr == ProductInfoAttribute.supplierRef)
			objectValue = obj.getSupplierRef();
		if(attr == ProductInfoAttribute.categoryRef)
			objectValue = obj.getCategoryRef();
		if(attr == ProductInfoAttribute.quantityPerUnit)
			objectValue = obj.getQuantityPerUnit();
		if(attr == ProductInfoAttribute.unitPrice)
			objectValue = obj.getUnitPrice();
		if(attr == ProductInfoAttribute.reorderLevel)
			objectValue = obj.getReorderLevel();
		if(attr == ProductInfoAttribute.discontinued)
			objectValue = obj.getDiscontinued();

		return operator.evaluate(objectValue, this.getValue());
	}
	private boolean evaluateStockInfo(StockInfo obj) {
		if(obj == null)
			return false;
		if(this.operator == null)
			return true;

		StockInfoAttribute attr = (StockInfoAttribute) this.attribute;
		Object objectValue = null;

		if(attr == StockInfoAttribute.id)
			objectValue = obj.getId();
		if(attr == StockInfoAttribute.unitsInStock)
			objectValue = obj.getUnitsInStock();
		if(attr == StockInfoAttribute.unitsOnOrder)
			objectValue = obj.getUnitsOnOrder();

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

	
}
