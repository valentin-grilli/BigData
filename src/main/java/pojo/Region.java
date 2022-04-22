package pojo;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;

public class Region extends LoggingPojo {

	private Integer id;
	private String description;

	public enum contains {
		region
	}
	private List<Territory> territoryList;

	// Empty constructor
	public Region() {}

	// Constructor on Identifier
	public Region(Integer id){
		this.id = id;
	}
	/*
	* Constructor on simple attribute 
	*/
	public Region(Integer id,String description) {
		this.id = id;
		this.description = description;
	}
	@Override
    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

	@Override
	public boolean equals(Object o){
		if(this==o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Region Region = (Region) o;
		boolean eqSimpleAttr = Objects.equals(id,Region.id) && Objects.equals(description,Region.description);
		boolean eqComplexAttr = false;
		eqComplexAttr = true && 
	Objects.equals(territoryList, Region.territoryList) &&
 true;
		return eqSimpleAttr && eqComplexAttr;
	}
	
	@Override
	public String toString(){
		return "Region { " + "id="+id +", "+
					"description="+description +"}"; 
	}
	
	public Integer getId() {
		return id;
	}

	public void setId(Integer id) {
		this.id = id;
	}
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	

	public List<Territory> _getTerritoryList() {
		return territoryList;
	}

	public void _setTerritoryList(List<Territory> territoryList) {
		this.territoryList = territoryList;
	}
}
