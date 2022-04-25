package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Composed_of extends LoggingPojo {

	private Order order;	
	private Product product;	
	private Double unitPrice;	
	private Integer quantity;	
	private Double discount;	

	//Empty constructor
	public Composed_of() {}
	
	//Role constructor
	public Composed_of(Order order,Product product){
		this.order=order;
		this.product=product;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Composed_of cloned = (Composed_of) super.clone();
		cloned.setOrder((Order)cloned.getOrder().clone());	
		cloned.setProduct((Product)cloned.getProduct().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "composed_of : "+
				"roles : {" +  "order:Order={"+order+"},"+ 
					 "product:Product={"+product+"}"+
				 "}"+
			" attributes : { " + "unitPrice="+unitPrice +", "+
					"quantity="+quantity +", "+
					"discount="+discount +"}"; 
	}
	public Order getOrder() {
		return order;
	}	

	public void setOrder(Order order) {
		this.order = order;
	}
	
	public Product getProduct() {
		return product;
	}	

	public void setProduct(Product product) {
		this.product = product;
	}
	
	public Double getUnitPrice() {
		return unitPrice;
	}

	public void setUnitPrice(Double unitPrice) {
		this.unitPrice = unitPrice;
	}
	
	public Integer getQuantity() {
		return quantity;
	}

	public void setQuantity(Integer quantity) {
		this.quantity = quantity;
	}
	
	public Double getDiscount() {
		return discount;
	}

	public void setDiscount(Double discount) {
		this.discount = discount;
	}
	

}
