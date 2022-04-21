package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class ProductInfo extends LoggingPojo {

	private Integer id;
	private String name;
	private Integer supplierRef;
	private Integer categoryRef;
	private String quantityPerUnit;
	private Double unitPrice;
	private Integer reorderLevel;
	private Boolean discontinued;


	// Empty constructor
	public ProductInfo() {}

	// Constructor on Identifier
	public ProductInfo(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public ProductInfo(Integer id,String name,Integer supplierRef,Integer categoryRef,String quantityPerUnit,Double unitPrice,Integer reorderLevel,Boolean discontinued) {
		this.id = id;
		this.name = name;
		this.supplierRef = supplierRef;
		this.categoryRef = categoryRef;
		this.quantityPerUnit = quantityPerUnit;
		this.unitPrice = unitPrice;
		this.reorderLevel = reorderLevel;
		this.discontinued = discontinued;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		ProductInfo ProductInfo = (ProductInfo) o;
		boolean eqSimpleAttr = Objects.equals(id,ProductInfo.id) && Objects.equals(name,ProductInfo.name) && Objects.equals(supplierRef,ProductInfo.supplierRef) && Objects.equals(categoryRef,ProductInfo.categoryRef) && Objects.equals(quantityPerUnit,ProductInfo.quantityPerUnit) && Objects.equals(unitPrice,ProductInfo.unitPrice) && Objects.equals(reorderLevel,ProductInfo.reorderLevel) && Objects.equals(discontinued,ProductInfo.discontinued);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "ProductInfo { " + "id="+id +", "+
					"name="+name +", "+
					"supplierRef="+supplierRef +", "+
					"categoryRef="+categoryRef +", "+
					"quantityPerUnit="+quantityPerUnit +", "+
					"unitPrice="+unitPrice +", "+
					"reorderLevel="+reorderLevel +", "+
					"discontinued="+discontinued +"}"; 
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

	

}
