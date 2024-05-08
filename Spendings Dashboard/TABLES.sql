#CREATE DATABASE SpendingsDB;

USE SpendingsDB;

CREATE TABLE Shops (
ID INT NOT NULL UNIQUE AUTO_INCREMENT,
Name varchar(255) NOT NULL,
City varchar(50) NOT NULL,
Street varchar(100),
Post_Code varchar(50),
PRIMARY KEY (ID)
);


CREATE TABLE Items (
ID INT NOT NULL UNIQUE,
Name Varchar(255) NOT NULL,
ItemGroup varchar(50)NOT NULL,
PRIMARY KEY (ID)
);


CREATE TABLE Sales (
	ReceiptID INT NOT NULL,
    ShopID INT NOT NULL,
    ItemID INT NOT NULL,
    TansactionDate DATE NOT NULL,
    ItemCost FLOAT(2) NOT NULL,
    ItemCount SMALLINT NOT NULL,
    ItemWeight FLOAT(5) NOT NULL,
    FOREIGN KEY (ShopID) REFERENCES Shops(ID),
    FOREIGN KEY (ItemID) REFERENCES Items(ID)
);




2023-11-17,moBilet,10,Bilet Autobusowy,2,0,10,Podróż