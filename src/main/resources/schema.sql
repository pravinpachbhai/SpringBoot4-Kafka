CREATE TABLE products (
                          id INT AUTO_INCREMENT PRIMARY KEY,
                          code VARCHAR(50) NOT NULL,
                          name VARCHAR(150) NOT NULL,
                          price DECIMAL(10,2) NOT NULL,
                          created TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                          UNIQUE (code)
);
