CREATE TABLE financial_transactions (
    transactionId VARCHAR(255),
    productId VARCHAR(255),
    productName VARCHAR(255),
    productCategory VARCHAR(255),
    productPrice FLOAT,
    productQuantity INT,
    productBrand VARCHAR(255),
    currency VARCHAR(255),
    customerId VARCHAR(255),
    transactionDate VARCHAR(255),  -- or DATETIME if you want to store as a date type
    paymentMethod VARCHAR(255),
    totalAmount FLOAT
);


