package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Insert extends LoggingPojo {

	private Supplier supplier;	
	private Product product;	

	//Empty constructor
	public Insert() {}
	
	//Role constructor
	public Insert(Supplier supplier,Product product){
		this.supplier=supplier;
		this.product=product;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Insert cloned = (Insert) super.clone();
		cloned.setSupplier((Supplier)cloned.getSupplier().clone());	
		cloned.setProduct((Product)cloned.getProduct().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "insert : "+
				"roles : {" +  "supplier:Supplier={"+supplier+"},"+ 
					 "product:Product={"+product+"}"+
				 "}"+
				"";
	}
	public Supplier getSupplier() {
		return supplier;
	}	

	public void setSupplier(Supplier supplier) {
		this.supplier = supplier;
	}
	
	public Product getProduct() {
		return product;
	}	

	public void setProduct(Product product) {
		this.product = product;
	}
	

}
