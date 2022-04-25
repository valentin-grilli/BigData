package tdo;

import pojo.Customer;
import java.util.List;
import java.util.ArrayList;

public class CustomerTDO extends Customer {
	private  String mongoSchema_Orders_customerRef_ID;
	public  String getMongoSchema_Orders_customerRef_ID() {
		return this.mongoSchema_Orders_customerRef_ID;
	}

	public void setMongoSchema_Orders_customerRef_ID(  String mongoSchema_Orders_customerRef_ID) {
		this.mongoSchema_Orders_customerRef_ID = mongoSchema_Orders_customerRef_ID;
	}

}
