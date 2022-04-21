package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Category extends LoggingPojo {

	private Integer id;
	private String categoryName;
	private String description;
	private String picture;


	// Empty constructor
	public Category() {}

	// Constructor on Identifier
	public Category(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Category(Integer id,String categoryName,String description,String picture) {
		this.id = id;
		this.categoryName = categoryName;
		this.description = description;
		this.picture = picture;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Category Category = (Category) o;
		boolean eqSimpleAttr = Objects.equals(id,Category.id) && Objects.equals(categoryName,Category.categoryName) && Objects.equals(description,Category.description) && Objects.equals(picture,Category.picture);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Category { " + "id="+id +", "+
					"categoryName="+categoryName +", "+
					"description="+description +", "+
					"picture="+picture +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getCategoryName() {
		return categoryName;
	}

	public void setCategoryName(String categoryName) {
		this.categoryName = categoryName;
	}
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	public String getPicture() {
		return picture;
	}

	public void setPicture(String picture) {
		this.picture = picture;
	}

	

}
