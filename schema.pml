databases {
	mysql relData {
		dbname : "reldata"
		host : "localhost"
		login : "root"
		password : "password"
		port : 3399
	}
	mongodb myMongoDB {
		dbname : "myMongoDB"
		host : "localhost"
		port : 27777
	}
	redis redisDB {
		host : "localhost"
		port : 6666
	}
}
//Conceptual schema
conceptual schema conceptualSchema {
	entity type Shipper {
		id : int,
		companyName : string, 
		phone : string
		identifier {
			id
		}
	}
	entity type ProductInfo {
		id : int, 
		name : string, 
		supplierRef : int,
		categoryRef : int,
		quantityPerUnit : string,
		unitPrice : float,
		reorderLevel : int,
		discontinued : bool
		identifier {
			id
		}
	}
	entity type StockInfo {
		id : int,
		unitsInStock : int,
		unitsOnOrder : int
		identifier {
			id
		}
	}
	entity type Customer {
		id : string,
		city : string,
		companyName : string, 
		contactName : string, 
		contactTitle : string,
		country : string, 
		fax : string, 
		phone : string, 
		postalCode : string,
		region : string, 
		address : string
		identifier {
			id
		}
	}
	entity type Order {
		id : int,
		freight : float,
		orderDate : date,
		requiredDate : date,
		shipAddress : string,
		shipCity : string,
		shipCountry : string,
		shipName : string,
		shipPostalCode : string, 
		shipRegion : string,
		shippedDate : date
		identifier {
			id
		}
	}
	entity type Category {
		id : int,
		categoryName : string,
		description : string,
		picture : string
		identifier {
			id
		}
	}
	//relationship
	relationship type make_by {
		order[0-N]: Order,
		client[1]: Customer
	}
	//relationship between ProductInfo and ProductStrock
	relationship type concern {
		stock[0-1]: StockInfo,
		product[0-1]: ProductInfo
	}
}
//Physical schema
physical schemas {
	key value schema kv : redisDB {
		kvpairs categoryPairs {
			key : "CATEGORY:"[categoryid],
			value : hash {
				CategoryName,
				Description,
				Picture
			}
		}
		kvpairs stockInfoPairs {
			key : "PRODUCT:"[productid]":STOCKINFO",
			value : hash {
				UnitsInStock,
				UnitsOnOrder
			}
			//Link to mysqlDB
			references {
				//name : link
				concerned : productid -> relSchema.ProductsInfo.ProductID
			}
		}
	}
	relational schema relSchema : relData {
		table Shippers {
			columns {
				ShipperID,
				CompanyName,
				Phone
			}
		}
		table ProductsInfo {
			columns {
				ProductID,
				ProductName,
				SupplierRef,
				CategoryRef,
				QuantityPerUnit,
				UnitPrice,
				ReorderLevel,
				Discontinued
			}
		}
	
	}
	document schema mongoSchema : myMongoDB {
		collection Customers {
			fields {
				City,
				CompanyName,
				ContactName,
				ContactTitle,
				Country,
				Fax,
				ID,
				Phone,
				PostalCode,
				Region,
				Address
			}
		}
		collection Orders {
			fields {
				EmployeeRef,
				Freight,
				OrderDate,
				RequiredDate,
				ShipAddress,
				OrderID,
				ShipCity,
				ShipCountry,
				ShipName,
				ShipPostalCode,
				ShipRegion,
				ShipVia,
				ShippedDate,
				customer[1]{
					CustomerID,
					ContactName
				}
			}
		}
	}
}
mapping rules {
	//Mapping StockInfo relationnal physical
	conceptualSchema.StockInfo(id, unitsInStock, unitsOnOrder) -> kv.stockInfoPairs(productid, UnitsInStock, UnitsOnOrder),
	//Mapping between StockInfo and ProductInfo
	conceptualSchema.ProductInfo(id) -> kv.stockInfoPairs(productid),
	conceptualSchema.concern.stock -> kv.stockInfoPairs.concerned,
	
	//Mapping Redis Category conceptual physical
	conceptualSchema.Category(id, categoryName, description, picture) -> kv.categoryPairs(categoryid, CategoryName, Description, Picture),
	
	//Mapping between conceptuel and physical
	conceptualSchema.Shipper(id, companyName, phone) -> relSchema.Shippers(ShipperID, CompanyName, Phone),
	conceptualSchema.ProductInfo(id, name, supplierRef, categoryRef,quantityPerUnit, unitPrice, reorderLevel, discontinued) -> relSchema.ProductsInfo(ProductID, ProductName, SupplierRef, CategoryRef, QuantityPerUnit, UnitPrice, ReorderLevel, Discontinued),
	conceptualSchema.Customer(id, city, companyName, contactName, contactTitle, country, fax, phone, postalCode, region, address) -> mongoSchema.Customers(ID, City, CompanyName, ContactName, ContactTitle, Country, Fax, Phone, PostalCode, Region, Address),
	conceptualSchema.Order(id, freight, orderDate, requiredDate, shipAddress, shipCity, shipCountry, shipName, shipPostalCode, shipRegion, shippedDate) 
	-> mongoSchema.Orders(OrderID, Freight, OrderDate, RequiredDate, ShipAddress, ShipCity, ShipCountry, ShipName, ShipPostalCode, ShipRegion, ShippedDate),
	conceptualSchema.Customer(id, companyName) -> mongoSchema.Orders.customer(CustomerID, ContactName),
	//relation Customer -> Order
	conceptualSchema.make_by.client -> mongoSchema.Orders.customer()
}