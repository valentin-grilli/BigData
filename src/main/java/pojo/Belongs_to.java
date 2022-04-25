package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Belongs_to extends LoggingPojo {

	private Product product;	
	private Category category;	

	//Empty constructor
	public Belongs_to() {}
	
	//Role constructor
	public Belongs_to(Product product,Category category){
		this.product=product;
		this.category=category;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Belongs_to cloned = (Belongs_to) super.clone();
		cloned.setProduct((Product)cloned.getProduct().clone());	
		cloned.setCategory((Category)cloned.getCategory().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "belongs_to : "+
				"roles : {" +  "product:Product={"+product+"},"+ 
					 "category:Category={"+category+"}"+
				 "}"+
				"";
	}
	public Product getProduct() {
		return product;
	}	

	public void setProduct(Product product) {
		this.product = product;
	}
	
	public Category getCategory() {
		return category;
	}	

	public void setCategory(Category category) {
		this.category = category;
	}
	

}
