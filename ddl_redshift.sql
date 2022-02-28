CREATE TABLE team1_schema.locations (
    BranchID VARCHAR(255) NOT NULL,
    BranchName VARCHAR(255) NOT NULL,
    PRIMARY KEY (BranchID)
);

CREATE TABLE team1_schema.payment_method (
    PaymentID VARCHAR(255) NOT NULL,
    PaymentMethod VARCHAR(255) NOT NULL,
    PRIMARY KEY (PaymentID),
    UNIQUE (PaymentID)
);

CREATE TABLE team1_schema.products (
    ProdID VARCHAR(255) NOT NULL,
    ProdName VARCHAR(255) NOT NULL,
    CurrentPrice DECIMAL(4,2),
    PRIMARY KEY (ProdID),
    UNIQUE (ProdID)
);

CREATE TABLE team1_schema.transactions (
    OrderID VARCHAR(255) NOT NULL,
    Time_Stamp TIMESTAMP,
    BranchID VARCHAR(255),
    PaymentID VARCHAR(255),
    Sum_Total DECIMAL(4,2),
    PRIMARY KEY (OrderID), 
    UNIQUE (OrderID),
    FOREIGN KEY (PaymentID) REFERENCES team1_schema.payment_method(PaymentID),
    FOREIGN KEY (BranchID) REFERENCES team1_schema.locations(BranchID)
);

CREATE TABLE team1_schema.order_products (
    OrderID VARCHAR(255) NOT NULL,
    ProdID VARCHAR(255) NOT NULL,
    Quantity INT NOT NULL,
    Price DECIMAL(4,2),
    FOREIGN KEY (OrderID) REFERENCES team1_schema.transactions(OrderID),
    FOREIGN KEY (ProdID) REFERENCES team1_schema.products(ProdID)
)