package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Product extends LoggingPojo {

	private Integer id;
	private String name;
	private Integer supplierRef;
	private Integer categoryRef;
	private String quantityPerUnit;
	private Double unitPrice;
	private Integer reorderLevel;
	private Boolean discontinued;
	private Integer unitsInStock;
	private Integer unitsOnOrder;

	public enum insert {
		product
	}
	private Supplier supplier;
	private List<Composed_of> composed_ofListAsProduct;
	public enum belongs_to {
		product
	}
	private Category category;

	// Empty constructor
	public Product() {}

	// Constructor on Identifier
	public Product(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Product(Integer id,String name,Integer supplierRef,Integer categoryRef,String quantityPerUnit,Double unitPrice,Integer reorderLevel,Boolean discontinued,Integer unitsInStock,Integer unitsOnOrder) {
		this.id = id;
		this.name = name;
		this.supplierRef = supplierRef;
		this.categoryRef = categoryRef;
		this.quantityPerUnit = quantityPerUnit;
		this.unitPrice = unitPrice;
		this.reorderLevel = reorderLevel;
		this.discontinued = discontinued;
		this.unitsInStock = unitsInStock;
		this.unitsOnOrder = unitsOnOrder;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Product Product = (Product) o;
		boolean eqSimpleAttr = Objects.equals(id,Product.id) && Objects.equals(name,Product.name) && Objects.equals(supplierRef,Product.supplierRef) && Objects.equals(categoryRef,Product.categoryRef) && Objects.equals(quantityPerUnit,Product.quantityPerUnit) && Objects.equals(unitPrice,Product.unitPrice) && Objects.equals(reorderLevel,Product.reorderLevel) && Objects.equals(discontinued,Product.discontinued) && Objects.equals(unitsInStock,Product.unitsInStock) && Objects.equals(unitsOnOrder,Product.unitsOnOrder);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(supplier, Product.supplier) &&
	Objects.equals(composed_ofListAsProduct,Product.composed_ofListAsProduct) &&
	Objects.equals(category, Product.category) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Product { " + "id="+id +", "+
					"name="+name +", "+
					"supplierRef="+supplierRef +", "+
					"categoryRef="+categoryRef +", "+
					"quantityPerUnit="+quantityPerUnit +", "+
					"unitPrice="+unitPrice +", "+
					"reorderLevel="+reorderLevel +", "+
					"discontinued="+discontinued +", "+
					"unitsInStock="+unitsInStock +", "+
					"unitsOnOrder="+unitsOnOrder +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}
	public Integer getSupplierRef() {
		return supplierRef;
	}

	public void setSupplierRef(Integer supplierRef) {
		this.supplierRef = supplierRef;
	}
	public Integer getCategoryRef() {
		return categoryRef;
	}

	public void setCategoryRef(Integer categoryRef) {
		this.categoryRef = categoryRef;
	}
	public String getQuantityPerUnit() {
		return quantityPerUnit;
	}

	public void setQuantityPerUnit(String quantityPerUnit) {
		this.quantityPerUnit = quantityPerUnit;
	}
	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}
	public Integer getReorderLevel() {
		return reorderLevel;
	}

	public void setReorderLevel(Integer reorderLevel) {
		this.reorderLevel = reorderLevel;
	}
	public Boolean getDiscontinued() {
		return discontinued;
	}

	public void setDiscontinued(Boolean discontinued) {
		this.discontinued = discontinued;
	}
	public Integer getUnitsInStock() {
		return unitsInStock;
	}

	public void setUnitsInStock(Integer unitsInStock) {
		this.unitsInStock = unitsInStock;
	}
	public Integer getUnitsOnOrder() {
		return unitsOnOrder;
	}

	public void setUnitsOnOrder(Integer unitsOnOrder) {
		this.unitsOnOrder = unitsOnOrder;
	}

	

	public Supplier _getSupplier() {
		return supplier;
	}

	public void _setSupplier(Supplier supplier) {
		this.supplier = supplier;
	}
	public java.util.List<Composed_of> _getComposed_ofListAsProduct() {
		return composed_ofListAsProduct;
	}

	public void _setComposed_ofListAsProduct(java.util.List<Composed_of> composed_ofListAsProduct) {
		this.composed_ofListAsProduct = composed_ofListAsProduct;
	}
	public Category _getCategory() {
		return category;
	}

	public void _setCategory(Category category) {
		this.category = category;
	}
}
