package pojo;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Contains extends LoggingPojo {

	private Territory territory;	
	private Region region;	

	//Empty constructor
	public Contains() {}
	
	//Role constructor
	public Contains(Territory territory,Region region){
		this.territory=territory;
		this.region=region;
	}
	

	@Override
    public Object clone() throws CloneNotSupportedException {
        Contains cloned = (Contains) super.clone();
		cloned.setTerritory((Territory)cloned.getTerritory().clone());	
		cloned.setRegion((Region)cloned.getRegion().clone());	
		return cloned;
    }

@Override
	public String toString(){
		return "contains : "+
				"roles : {" +  "territory:Territory={"+territory+"},"+ 
					 "region:Region={"+region+"}"+
				 "}"+
				"";
	}
	public Territory getTerritory() {
		return territory;
	}	

	public void setTerritory(Territory territory) {
		this.territory = territory;
	}
	
	public Region getRegion() {
		return region;
	}	

	public void setRegion(Region region) {
		this.region = region;
	}
	

}
