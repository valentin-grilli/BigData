package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Concern extends LoggingPojo {

	private StockInfo stock;	
	private ProductInfo product;	

	//Empty constructor
	public Concern() {}
	
	//Role constructor
	public Concern(StockInfo stock,ProductInfo product){
		this.stock=stock;
		this.product=product;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Concern cloned = (Concern) super.clone();
		cloned.setStock((StockInfo)cloned.getStock().clone());	
		cloned.setProduct((ProductInfo)cloned.getProduct().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "concern : "+
				"roles : {" +  "stock:StockInfo={"+stock+"},"+ 
					 "product:ProductInfo={"+product+"}"+
				 "}"+
				"";
	}
	public StockInfo getStock() {
		return stock;
	}	

	public void setStock(StockInfo stock) {
		this.stock = stock;
	}
	
	public ProductInfo getProduct() {
		return product;
	}	

	public void setProduct(ProductInfo product) {
		this.product = product;
	}
	

}
