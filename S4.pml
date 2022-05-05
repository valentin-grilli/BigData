databases {
	mysql myRelDB {
		dbname : "reldata"
		host : "localhost"
		port : 3399
		login : "root"
		password : "password"
	}
	
	mongodb myMongoDB{
		dbname : "myMongoDB"
		host : "localhost"
		port : 27777
	}
	
	redis myRedisDB{
		host : "localhost"
		port : 6666
	}
}

conceptual schema csmodel {
	

entity type Orders {
	id : int,
   orderDate : date,
   requiredDate : date,
   shippedDate : date,
   freight : float,
   shipName : string,
   shipAddress : string,
   shipCity : string,
   shipRegion : string,
   shipPostalCode : string,
   shipCountry : string
   identifier{
   	id
   }
}


entity type Products {
   productId : int,
   productName : string,
   quantityPerUnit : string,
   unitPrice : float,
   unitsInStock : int,
   unitsOnOrder : int,
   reorderLevel : int,
   discontinued : bool
   identifier{
   	productId
   }
}

entity type Suppliers {
   supplierId : int,
   companyName : string,
   contactName : string,
   contactTitle : string,
   address : string,
   city : string,
   region : string,
   postalCode : string,
   country : string,
   phone : string,
   fax : string,
   homePage : text
   identifier{
   	supplierId
   }
}

entity type Customers {
   customerID : string,
   companyName : string,
   contactName : string,
   contactTitle : string,
   address : string,
   city : string,
   region : string,
   postalCode : string,
   country : string,
   phone : string,
   fax : string
   identifier {
   	customerID
   }
}

entity type Categories {
   categoryID : int,
   categoryName : string,
   description : text,
   picture : blob
   identifier {
   	categoryID
   }
}

entity type Shippers {
   shipperID : int,
   companyName : string,
   phone : string
   identifier{
   	shipperID
   }
}

entity type Employees {
   employeeID : int,
   lastName : string,
   firstName : string,
   title : string,
   titleOfCourtesy : string,
   birthDate : date,
   hireDate : date,
   address : string,
   city : string,
   region : string,
   postalCode : string,
   country : string,
   homePhone : string,
   extension : string,
   photo : blob,
   notes : text,
   photoPath : string,
   salary : float
   identifier{
   	employeeID
   }
}


entity type Region {
   regionID : int,
   regionDescription : string
   identifier{
   	regionID
   }
}

entity type Territories {
   territoryID : string,
   territoryDescription : string
   identifier {
   	territoryID
   }
}

relationship type locatedIn {
	territories[1] : Territories, 
	region[0-N] : Region
}

relationship type works {
	employed[0-N] : Employees,
	territories[0-N] : Territories 
}

relationship type reportsTo{
	subordonee[0-1] : Employees,
	boss[0-N] : Employees
}

relationship type supply {
	suppliedProduct[0-1] : Products,
	supplier[0-N] : Suppliers
}

relationship type typeOf {
	product[0-1] : Products,
	category[0-N] : Categories 
}

relationship type buy {
	boughtOrder[1] : Orders,
	customer[0-N] : Customers 
}

relationship type register {
	processedOrder[1] : Orders,
	employeeInCharge[0-N] : Employees 
}

relationship type ships {
	shippedOrder[1] : Orders,
	shipper[0-N] : Shippers
}

relationship type composedOf {
	order[0-N] : Orders,
	orderedProducts[0-N] : Products,
	unitPrice : float,
	quantity : int,
	discount : float 
}


}
physical schemas {
	document schema mongoDB : myMongoDB{
		collection Orders {
			fields{
		   	OrderID,
		   	OrderDate,
		    RequiredDate,
		   	ShippedDate,
		   	Freight,
		   	ShipName,
		   	ShipAddress,
		   	ShipCity,
		   	ShipRegion,
		   	ShipPostalCode,
		   	ShipCountry,
		   	EmployeeRef,
		   	ShipVia,
			customer [1] {
			   CustomerID,
			   ContactName
				}
			}
			references{
				encoded : EmployeeRef -> Employees.EmployeeID
				deliver : ShipVia -> relDB.Shippers.ShipperID
			}
		}
		
		collection Customers {
			fields{
			   ID,
			   CompanyName,
			   ContactName,
			   ContactTitle,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   Phone,
			   Fax
			}
		}
		
		collection Suppliers {
			fields {
			   SupplierID,
			   CompanyName,
			   ContactName,
			   ContactTitle,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   Phone,
			   Fax,
			   HomePage
			}
		}
		
		collection Employees {
			fields{
			   EmployeeID,
			   LastName,
			   FirstName,
			   Title,
			   TitleOfCourtesy,
			   BirthDate,
			   HireDate,
			   HomePhone,
			   Extension,
			   Photo,
			   Notes,
			   PhotoPath,
			   Salary,
			   Address,
			   City,
			   Region,
			   PostalCode,
			   Country,
			   ReportsTo,
			   territories [0-N] {
			   	TerritoryID,
			   	TerritoryDescription,
			   	region[1]{
				   RegionID,
				   RegionDescription
			   	}
			   }
			}
			references{
				manager : ReportsTo -> EmployeeID
			}
			
		}
	}
	
	key value schema kvDB : myRedisDB{
		
		kvpairs categoriesKV {
			key : "CATEGORY:"[catid],
			value : hash {
			   CategoryName,
			   Description,
			   Picture
			}
		}		
		kvpairs ProductsStockInfo {
			key : "PRODUCT:"[ProductID]":STOCKINFO",
			value : hash {
			   UnitsInStock,
			   UnitsOnOrder
			}
		}
	}
	
	relational schema relDB : myRelDB {

		table ProductsInfo {
			columns{
			   ProductID,
			   ProductName,
			   QuantityPerUnit,
			   UnitPrice,
			   ReorderLevel,
			   Discontinued,
			   SupplierRef,
			   CategoryRef
			}
			references{
				supply : SupplierRef -> mongoDB.Suppliers.SupplierID
				isCategory : CategoryRef -> kvDB.categoriesKV.catid
			}
		}

		
		table Shippers {
			columns{
			   ShipperID,
			   CompanyName,
			   Phone
			}
		}
		

		table Order_Details {
			columns{
				OrderRef,
				ProductRef,
				UnitPrice,
				Quantity,
				Discount
			}
			references{
				order : OrderRef -> mongoDB.Orders.OrderID
				purchasedProducts : ProductRef -> ProductsInfo.ProductID
			}
		}
		
	}
}

mapping rules {
	csmodel.Orders(id, freight, orderDate,requiredDate, shipAddress, shipCity, shipCountry, shipName, shippedDate, shipPostalCode, shipRegion) -> mongoDB.Orders(OrderID,Freight,OrderDate,RequiredDate,ShipAddress,ShipCity, ShipCountry, ShipName, ShippedDate, ShipPostalCode, ShipRegion),
	csmodel.Products( productId,discontinued,productName,quantityPerUnit,reorderLevel,unitPrice) -> relDB.ProductsInfo( ProductID,Discontinued,ProductName,QuantityPerUnit,ReorderLevel,UnitPrice),
	csmodel.Products( productId,unitsInStock,unitsOnOrder) -> kvDB.ProductsStockInfo( ProductID,UnitsInStock,UnitsOnOrder),
	csmodel.Suppliers( supplierId,address,city,companyName,contactName,contactTitle,country,fax,homePage,phone,postalCode,region) -> mongoDB.Suppliers( SupplierID,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,HomePage,Phone,PostalCode,Region), 
	csmodel.Customers(customerID,contactName) -> mongoDB.Orders.customer(CustomerID, ContactName),
	csmodel.Customers(customerID,address,city,companyName,contactName,contactTitle,country,fax,phone,postalCode,region) -> mongoDB.Customers(ID,Address,City,CompanyName,ContactName,ContactTitle,Country,Fax,Phone,PostalCode,Region),
	csmodel.Categories( categoryID,categoryName,description,picture) -> kvDB.categoriesKV( catid,CategoryName,Description,Picture),
	csmodel.Shippers( shipperID,companyName,phone) -> relDB.Shippers(ShipperID,CompanyName,Phone),
	csmodel.Employees( employeeID,address,birthDate,city,country,extension,firstName,lastName,hireDate,homePhone,notes,photo,photoPath,postalCode,region,salary,title,titleOfCourtesy)
		->
		mongoDB.Employees( EmployeeID,Address,BirthDate,City,Country,Extension,FirstName,LastName,HireDate,HomePhone,Notes,Photo,PhotoPath,PostalCode,Region,Salary,Title,TitleOfCourtesy),
	csmodel.reportsTo.subordonee -> mongoDB.Employees.manager,  
	csmodel.Region( regionID,regionDescription) -> mongoDB.Employees.territories.region( RegionID,RegionDescription),
	csmodel.Territories( territoryID,territoryDescription) -> mongoDB.Employees.territories( TerritoryID,TerritoryDescription),
	csmodel.supply.suppliedProduct -> relDB.ProductsInfo.supply,
	csmodel.typeOf.product -> relDB.ProductsInfo.isCategory,
	csmodel.locatedIn.territories -> mongoDB.Employees.territories.region(),
	csmodel.ships.shippedOrder -> mongoDB.Orders.deliver, 
	csmodel.register.processedOrder -> mongoDB.Orders.encoded,
	csmodel.buy.boughtOrder -> mongoDB.Orders.customer(),
	rel : csmodel.composedOf( discount,quantity,unitPrice) -> relDB.Order_Details( Discount,Quantity,UnitPrice),
	csmodel.composedOf.orderedProducts -> relDB.Order_Details.purchasedProducts,
	csmodel.composedOf.order -> relDB.Order_Details.order,
	csmodel.works.employed -> mongoDB.Employees.territories()
	
}