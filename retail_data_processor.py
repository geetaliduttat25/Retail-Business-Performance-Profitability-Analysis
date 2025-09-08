#!/usr/bin/env python3
"""
Retail Business Performance & Profitability Analysis
Converts CSV data to SQL and uploads to MySQL database
By : Geetali Dutta
"""

import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RetailDataProcessor:
    def __init__(self, host='localhost', port=3306, username='root', password='root', database='retail_analysis'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
    def connect_to_mysql(self):
        """Establish connection to MySQL server"""
        try:
            # First connect without database to create it if needed
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password
            )
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to MySQL server")
            return True
        except Error as e:
            logger.error(f"Error connecting to MySQL: {e}")
            return False
    
    def create_database(self):
        """Create the retail_analysis database if it doesn't exist"""
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            self.cursor.execute(f"USE {self.database}")
            logger.info(f"Database '{self.database}' created/selected successfully")
            return True
        except Error as e:
            logger.error(f"Error creating database: {e}")
            return False
    
    def create_table_schema(self):
        """Create the retail_inventory table with appropriate schema"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS retail_inventory (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE NOT NULL,
            store_id VARCHAR(10) NOT NULL,
            product_id VARCHAR(10) NOT NULL,
            category VARCHAR(50) NOT NULL,
            region VARCHAR(20) NOT NULL,
            inventory_level INT NOT NULL,
            units_sold INT NOT NULL,
            units_ordered INT NOT NULL,
            demand_forecast DECIMAL(10,2) NOT NULL,
            price DECIMAL(10,2) NOT NULL,
            discount INT NOT NULL,
            weather_condition VARCHAR(20) NOT NULL,
            holiday_promotion TINYINT NOT NULL,
            competitor_pricing DECIMAL(10,2) NOT NULL,
            seasonality VARCHAR(20) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_date (date),
            INDEX idx_store_product (store_id, product_id),
            INDEX idx_category (category),
            INDEX idx_region (region),
            INDEX idx_seasonality (seasonality)
        )
        """
        
        try:
            self.cursor.execute(create_table_query)
            logger.info("Table 'retail_inventory' created successfully")
            return True
        except Error as e:
            logger.error(f"Error creating table: {e}")
            return False
    
    def load_and_process_csv(self, csv_file_path):
        """Load CSV file and process data for SQL insertion"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            logger.info(f"Loaded CSV file with {len(df)} records")
            
            # Convert date format - handle different formats flexibly
            df['Date'] = pd.to_datetime(df['Date'], format='mixed', dayfirst=False).dt.strftime('%Y-%m-%d')
            
            # Clean column names (replace spaces with underscores, lowercase)
            df.columns = [col.replace(' ', '_').replace('/', '_').lower() for col in df.columns]
            
            # Handle any missing values
            df = df.fillna(0)
            
            logger.info("Data processed successfully")
            return df
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            return None
    
    def insert_data_batch(self, df, batch_size=1000):
        """Insert data into MySQL table in batches"""
        insert_query = """
        INSERT INTO retail_inventory 
        (date, store_id, product_id, category, region, inventory_level, 
         units_sold, units_ordered, demand_forecast, price, discount, 
         weather_condition, holiday_promotion, competitor_pricing, seasonality)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            total_records = len(df)
            inserted_records = 0
            
            # Process data in batches
            for start_idx in range(0, total_records, batch_size):
                end_idx = min(start_idx + batch_size, total_records)
                batch_df = df.iloc[start_idx:end_idx]
                
                # Prepare batch data
                batch_data = []
                for _, row in batch_df.iterrows():
                    batch_data.append((
                        row['date'],
                        row['store_id'],
                        row['product_id'],
                        row['category'],
                        row['region'],
                        int(row['inventory_level']),
                        int(row['units_sold']),
                        int(row['units_ordered']),
                        float(row['demand_forecast']),
                        float(row['price']),
                        int(row['discount']),
                        row['weather_condition'],
                        int(row['holiday_promotion']),
                        float(row['competitor_pricing']),
                        row['seasonality']
                    ))
                
                # Execute batch insert
                self.cursor.executemany(insert_query, batch_data)
                self.connection.commit()
                
                inserted_records += len(batch_data)
                logger.info(f"Inserted {inserted_records}/{total_records} records")
            
            logger.info(f"Successfully inserted all {total_records} records")
            return True
            
        except Error as e:
            logger.error(f"Error inserting data: {e}")
            self.connection.rollback()
            return False
    
    def create_profit_analysis_views(self):
        """Create SQL views for profit analysis"""
        views = {
            'profit_by_category': """
            CREATE OR REPLACE VIEW profit_by_category AS
            SELECT 
                category,
                COUNT(*) as total_transactions,
                SUM(units_sold) as total_units_sold,
                SUM(units_sold * price) as total_revenue,
                SUM(units_sold * price * discount / 100) as total_discount_amount,
                SUM(units_sold * price * (1 - discount / 100.0)) as net_revenue,
                AVG(price) as avg_price,
                AVG(discount) as avg_discount_percent,
                SUM(units_sold * price * (1 - discount / 100.0)) / SUM(units_sold * price) * 100 as profit_margin_percent
            FROM retail_inventory 
            WHERE units_sold > 0
            GROUP BY category
            ORDER BY net_revenue DESC
            """,
            
            'inventory_turnover_analysis': """
            CREATE OR REPLACE VIEW inventory_turnover_analysis AS
            SELECT 
                category,
                region,
                AVG(inventory_level) as avg_inventory_level,
                SUM(units_sold) as total_units_sold,
                CASE 
                    WHEN AVG(inventory_level) > 0 
                    THEN SUM(units_sold) / AVG(inventory_level) 
                    ELSE 0 
                END as inventory_turnover_ratio,
                AVG(CASE WHEN inventory_level > units_sold THEN inventory_level - units_sold ELSE 0 END) as avg_overstock
            FROM retail_inventory
            GROUP BY category, region
            ORDER BY inventory_turnover_ratio DESC
            """,
            
            'seasonal_performance': """
            CREATE OR REPLACE VIEW seasonal_performance AS
            SELECT 
                seasonality,
                category,
                COUNT(*) as transactions,
                SUM(units_sold) as total_units_sold,
                SUM(units_sold * price * (1 - discount / 100.0)) as net_revenue,
                AVG(price) as avg_price,
                AVG(demand_forecast) as avg_demand_forecast,
                AVG(units_sold / NULLIF(demand_forecast, 0)) as demand_fulfillment_ratio
            FROM retail_inventory
            WHERE units_sold > 0 AND demand_forecast > 0
            GROUP BY seasonality, category
            ORDER BY seasonality, net_revenue DESC
            """
        }
        
        try:
            for view_name, view_query in views.items():
                self.cursor.execute(view_query)
                logger.info(f"Created view: {view_name}")
            
            self.connection.commit()
            return True
        except Error as e:
            logger.error(f"Error creating views: {e}")
            return False
    
    def generate_summary_report(self):
        """Generate a summary report of the uploaded data"""
        try:
            # Basic statistics
            self.cursor.execute("SELECT COUNT(*) FROM retail_inventory")
            total_records = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(DISTINCT store_id) FROM retail_inventory")
            unique_stores = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(DISTINCT product_id) FROM retail_inventory")
            unique_products = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT COUNT(DISTINCT category) FROM retail_inventory")
            unique_categories = self.cursor.fetchone()[0]
            
            self.cursor.execute("SELECT MIN(date), MAX(date) FROM retail_inventory")
            date_range = self.cursor.fetchone()
            
            print("\n" + "="*60)
            print("RETAIL DATA UPLOAD SUMMARY")
            print("="*60)
            print(f"Total Records Uploaded: {total_records:,}")
            print(f"Unique Stores: {unique_stores}")
            print(f"Unique Products: {unique_products}")
            print(f"Product Categories: {unique_categories}")
            print(f"Date Range: {date_range[0]} to {date_range[1]}")
            print("="*60)
            
            # Top categories by revenue
            print("\nTOP 5 CATEGORIES BY NET REVENUE:")
            print("-" * 40)
            self.cursor.execute("""
                SELECT category, 
                       FORMAT(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue
                FROM retail_inventory 
                WHERE units_sold > 0
                GROUP BY category 
                ORDER BY SUM(units_sold * price * (1 - discount / 100.0)) DESC 
                LIMIT 5
            """)
            
            for row in self.cursor.fetchall():
                print(f"{row[0]:<15}: ${row[1]}")
            
            print("\nDatabase setup completed successfully!")
            print("You can now run SQL queries for further analysis.")
            
        except Error as e:
            logger.error(f"Error generating summary: {e}")
    
    def close_connection(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def process_retail_data(self, csv_file_path):
        """Main method to process retail data from CSV to MySQL"""
        try:
            # Step 1: Connect to MySQL
            if not self.connect_to_mysql():
                return False
            
            # Step 2: Create database
            if not self.create_database():
                return False
            
            # Step 3: Create table schema
            if not self.create_table_schema():
                return False
            
            # Step 4: Load and process CSV data
            df = self.load_and_process_csv(csv_file_path)
            if df is None:
                return False
            
            # Step 5: Insert data into database
            if not self.insert_data_batch(df):
                return False
            
            # Step 6: Create analysis views
            if not self.create_profit_analysis_views():
                return False
            
            # Step 7: Generate summary report
            self.generate_summary_report()
            
            return True
            
        except Exception as e:
            logger.error(f"Error in main process: {e}")
            return False
        finally:
            self.close_connection()

def main():
    """Main function"""
    csv_file_path = '/Users/geetali/Desktop/Geetali/retail_store_inventory.csv'
    
    # Initialize processor
    processor = RetailDataProcessor(
        host='localhost',
        port=3306,
        username='root',
        password='root',
        database='retail_analysis'
    )
    
    # Process the data
    success = processor.process_retail_data(csv_file_path)
    
    if success:
        print("\n Retail data processing completed successfully!")
        print("\nNext steps:")
        print("1. Run SQL queries for profit margin analysis")
        print("2. Analyze correlation between inventory and profitability")
        print("3. Create Tableau dashboard with the uploaded data")
        print("By : Geetali Dutta")
    else:
        print("\n Error occurred during data processing. Check logs for details.")
        sys.exit(1)

if __name__ == "__main__":
    main()