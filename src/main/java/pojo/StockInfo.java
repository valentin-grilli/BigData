package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class StockInfo extends LoggingPojo {

	private Integer id;
	private Integer unitsInStock;
	private Integer unitsOnOrder;

	public enum concern {
		stock
	}
	private ProductInfo product;

	// Empty constructor
	public StockInfo() {}

	// Constructor on Identifier
	public StockInfo(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public StockInfo(Integer id,Integer unitsInStock,Integer unitsOnOrder) {
		this.id = id;
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
		StockInfo StockInfo = (StockInfo) o;
		boolean eqSimpleAttr = Objects.equals(id,StockInfo.id) && Objects.equals(unitsInStock,StockInfo.unitsInStock) && Objects.equals(unitsOnOrder,StockInfo.unitsOnOrder);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(product, StockInfo.product) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "StockInfo { " + "id="+id +", "+
					"unitsInStock="+unitsInStock +", "+
					"unitsOnOrder="+unitsOnOrder +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
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

	

	public ProductInfo _getProduct() {
		return product;
	}

	public void _setProduct(ProductInfo product) {
		this.product = product;
	}
}
