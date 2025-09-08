#!/usr/bin/env python3
"""
Retail Business Performance & Profitability Analysis
Correlation Analysis and Strategic Recommendations

This script performs advanced analytics on retail inventory data to:
1. Analyze correlation between inventory levels and profitability
2. Identify slow-moving and overstocked items
3. Generate strategic recommendations for inventory optimization

By : Geetali Dutta
"""

import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class RetailAnalytics:
    def __init__(self, host='localhost', port=3306, user='root', password='root', database='retail_analysis'):
        self.connection_params = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database
        }
        self.df = None
        
    def connect_to_database(self):
        """Establish connection to MySQL database"""
        try:
            connection = mysql.connector.connect(**self.connection_params)
            return connection
        except Error as e:
            print(f"Error connecting to MySQL: {e}")
            return None
    
    def load_data(self):
        """Load data from MySQL database"""
        connection = self.connect_to_database()
        if connection:
            try:
                query = """
                SELECT 
                    date, store_id, product_id, category, region, seasonality,
                    inventory_level, units_sold, price, discount, 
                    competitor_pricing, demand_forecast, weather_condition,
                    holiday_promotion,
                    (units_sold * price) as gross_revenue,
                    (units_sold * price * (1 - discount / 100.0)) as net_revenue,
                    (inventory_level - units_sold) as overstock_units,
                    CASE WHEN inventory_level > 0 THEN units_sold / inventory_level ELSE 0 END as turnover_ratio,
                    CASE WHEN units_sold > 0 THEN (units_sold * price * (1 - discount / 100.0)) / (units_sold * price) ELSE 0 END as profit_margin
                FROM retail_inventory
                WHERE inventory_level > 0 AND price > 0
                """
                
                self.df = pd.read_sql(query, connection)
                print(f"Loaded {len(self.df)} records for analysis")
                
                # Convert date column
                self.df['date'] = pd.to_datetime(self.df['date'])
                
                connection.close()
                return True
                
            except Error as e:
                print(f"Error loading data: {e}")
                connection.close()
                return False
        return False
    
    def calculate_inventory_metrics(self):
        """Calculate advanced inventory metrics"""
        if self.df is None:
            print("No data loaded. Please run load_data() first.")
            return
        
        # Calculate inventory days (assuming monthly data)
        self.df['inventory_days'] = self.df['inventory_level'] / (self.df['units_sold'] + 0.1) * 30
        
        # Calculate profit per unit
        self.df['profit_per_unit'] = self.df['net_revenue'] / (self.df['units_sold'] + 0.1)
        
        # Calculate inventory efficiency score
        self.df['efficiency_score'] = (self.df['turnover_ratio'] * self.df['profit_margin']) * 100
        
        # Categorize inventory performance
        self.df['inventory_category'] = pd.cut(
            self.df['inventory_days'],
            bins=[0, 15, 30, 60, float('inf')],
            labels=['Fast Moving', 'Normal', 'Slow Moving', 'Dead Stock']
        )
        
        print("Inventory metrics calculated successfully")
    
    def correlation_analysis(self):
        """Perform correlation analysis between inventory and profitability metrics"""
        if self.df is None:
            print("No data loaded. Please run load_data() first.")
            return
        
        print("\n" + "="*60)
        print("CORRELATION ANALYSIS: INVENTORY vs PROFITABILITY")
        print("="*60)
        
        # Select numeric columns for correlation
        numeric_cols = ['inventory_level', 'units_sold', 'inventory_days', 'turnover_ratio', 
                       'net_revenue', 'profit_margin', 'profit_per_unit', 'efficiency_score',
                       'price', 'discount', 'demand_forecast']
        
        correlation_matrix = self.df[numeric_cols].corr()
        
        # Key correlations to analyze
        key_correlations = {
            'Inventory Days vs Profit Margin': correlation_matrix.loc['inventory_days', 'profit_margin'],
            'Inventory Days vs Net Revenue': correlation_matrix.loc['inventory_days', 'net_revenue'],
            'Turnover Ratio vs Profit Margin': correlation_matrix.loc['turnover_ratio', 'profit_margin'],
            'Inventory Level vs Efficiency Score': correlation_matrix.loc['inventory_level', 'efficiency_score'],
            'Discount vs Profit Margin': correlation_matrix.loc['discount', 'profit_margin'],
            'Demand Forecast vs Units Sold': correlation_matrix.loc['demand_forecast', 'units_sold']
        }
        
        print("\nKEY CORRELATION FINDINGS:")
        print("-" * 40)
        for relationship, correlation in key_correlations.items():
            strength = self._interpret_correlation(correlation)
            print(f"{relationship:<35}: {correlation:>6.3f} ({strength})")
        
        # Statistical significance testing
        print("\n\nSTATISTICAL SIGNIFICANCE TESTS:")
        print("-" * 40)
        
        # Test key relationships
        relationships_to_test = [
            ('inventory_days', 'profit_margin'),
            ('turnover_ratio', 'net_revenue'),
            ('inventory_level', 'efficiency_score')
        ]
        
        for var1, var2 in relationships_to_test:
            # Remove NaN values for correlation test
            clean_data = self.df[[var1, var2]].dropna()
            if len(clean_data) > 10:
                correlation, p_value = stats.pearsonr(clean_data[var1], clean_data[var2])
                significance = "Significant" if p_value < 0.05 else "Not Significant"
                print(f"{var1} vs {var2}:")
                print(f"  Correlation: {correlation:.3f}, p-value: {p_value:.4f} ({significance})")
        
        return correlation_matrix
    
    def _interpret_correlation(self, correlation):
        """Interpret correlation strength"""
        abs_corr = abs(correlation)
        if abs_corr >= 0.7:
            return "Strong"
        elif abs_corr >= 0.3:
            return "Moderate"
        elif abs_corr >= 0.1:
            return "Weak"
        else:
            return "Very Weak"
    
    def identify_problem_areas(self):
        """Identify slow-moving and overstocked items"""
        if self.df is None:
            print("No data loaded. Please run load_data() first.")
            return
        
        print("\n" + "="*60)
        print("PROBLEM AREA IDENTIFICATION")
        print("="*60)
        
        # Slow-moving items (high inventory days, low turnover)
        slow_moving = self.df[
            (self.df['inventory_days'] > 45) & 
            (self.df['turnover_ratio'] < 0.3)
        ].copy()
        
        # Overstocked items (high overstock units, low efficiency)
        overstocked = self.df[
            (self.df['overstock_units'] > self.df['overstock_units'].quantile(0.8)) &
            (self.df['efficiency_score'] < self.df['efficiency_score'].quantile(0.3))
        ].copy()
        
        # Dead stock (very high inventory days, minimal sales)
        dead_stock = self.df[
            (self.df['inventory_days'] > 90) & 
            (self.df['units_sold'] <= 1)
        ].copy()
        
        print(f"\nPROBLEM INVENTORY SUMMARY:")
        print(f"Slow-Moving Items: {len(slow_moving)} products")
        print(f"Overstocked Items: {len(overstocked)} products")
        print(f"Dead Stock Items: {len(dead_stock)} products")
        
        # Category-wise analysis
        print("\nSLOW-MOVING ITEMS BY CATEGORY:")
        print("-" * 40)
        if len(slow_moving) > 0:
            category_analysis = slow_moving.groupby('category').agg({
                'product_id': 'count',
                'inventory_days': 'mean',
                'net_revenue': 'sum',
                'overstock_units': 'sum'
            }).round(2)
            category_analysis.columns = ['Count', 'Avg_Inventory_Days', 'Total_Revenue', 'Total_Overstock']
            print(category_analysis.sort_values('Count', ascending=False))
        
        # Regional analysis
        print("\nOVERSTOCKED ITEMS BY REGION:")
        print("-" * 40)
        if len(overstocked) > 0:
            region_analysis = overstocked.groupby('region').agg({
                'product_id': 'count',
                'overstock_units': 'sum',
                'net_revenue': 'sum',
                'efficiency_score': 'mean'
            }).round(2)
            region_analysis.columns = ['Count', 'Total_Overstock', 'Lost_Revenue', 'Avg_Efficiency']
            print(region_analysis.sort_values('Total_Overstock', ascending=False))
        
        return {
            'slow_moving': slow_moving,
            'overstocked': overstocked,
            'dead_stock': dead_stock
        }
    
    def seasonal_analysis(self):
        """Analyze seasonal patterns and their impact on inventory"""
        if self.df is None:
            print("No data loaded. Please run load_data() first.")
            return
        
        print("\n" + "="*60)
        print("SEASONAL INVENTORY ANALYSIS")
        print("="*60)
        
        seasonal_metrics = self.df.groupby(['seasonality', 'category']).agg({
            'inventory_days': 'mean',
            'turnover_ratio': 'mean',
            'profit_margin': 'mean',
            'net_revenue': 'sum',
            'units_sold': 'sum',
            'overstock_units': 'sum'
        }).round(3)
        
        print("\nSEASONAL PERFORMANCE BY CATEGORY:")
        print("-" * 50)
        print(seasonal_metrics)
        
        # Weather impact analysis
        if 'weather_condition' in self.df.columns:
            print("\nWEATHER IMPACT ON INVENTORY TURNOVER:")
            print("-" * 45)
            weather_impact = self.df.groupby('weather_condition').agg({
                'turnover_ratio': 'mean',
                'units_sold': 'mean',
                'inventory_days': 'mean'
            }).round(3)
            print(weather_impact.sort_values('turnover_ratio', ascending=False))
    
    def generate_strategic_recommendations(self):
        """Generate strategic recommendations based on analysis"""
        if self.df is None:
            print("No data loaded. Please run load_data() first.")
            return
        
        print("\n" + "="*80)
        print("STRATEGIC RECOMMENDATIONS FOR INVENTORY OPTIMIZATION")
        print("="*80)
        
        recommendations = []
        
        # Analyze overall performance
        avg_inventory_days = self.df['inventory_days'].mean()
        avg_turnover = self.df['turnover_ratio'].mean()
        avg_profit_margin = self.df['profit_margin'].mean()
        
        print(f"\nCURRENT PERFORMANCE METRICS:")
        print(f"Average Inventory Days: {avg_inventory_days:.1f} days")
        print(f"Average Turnover Ratio: {avg_turnover:.3f}")
        print(f"Average Profit Margin: {avg_profit_margin:.1%}")
        
        # Category-specific recommendations
        category_performance = self.df.groupby('category').agg({
            'inventory_days': 'mean',
            'turnover_ratio': 'mean',
            'profit_margin': 'mean',
            'net_revenue': 'sum',
            'overstock_units': 'sum'
        }).round(3)
        
        print("\n" + "="*60)
        print("CATEGORY-SPECIFIC STRATEGIC RECOMMENDATIONS")
        print("="*60)
        
        for category in category_performance.index:
            cat_data = category_performance.loc[category]
            print(f"\n{category.upper()} CATEGORY:")
            print("-" * 40)
            
            # High inventory days
            if cat_data['inventory_days'] > avg_inventory_days * 1.2:
                recommendations.append(f"{category}: Reduce inventory levels - currently {cat_data['inventory_days']:.1f} days")
                print(f"HIGH INVENTORY RISK: {cat_data['inventory_days']:.1f} days (vs avg {avg_inventory_days:.1f})")
                print(f"   → Implement just-in-time ordering")
                print(f"   → Review supplier lead times")
                print(f"   → Consider promotional campaigns")
            
            # Low turnover
            if cat_data['turnover_ratio'] < avg_turnover * 0.8:
                recommendations.append(f"{category}: Improve turnover ratio - currently {cat_data['turnover_ratio']:.3f}")
                print(f"LOW TURNOVER: {cat_data['turnover_ratio']:.3f} (vs avg {avg_turnover:.3f})")
                print(f"   → Optimize product mix")
                print(f"   → Enhance marketing efforts")
                print(f"   → Review pricing strategy")
            
            # High overstock
            if cat_data['overstock_units'] > self.df['overstock_units'].quantile(0.7):
                recommendations.append(f"{category}: Address overstock of {cat_data['overstock_units']:.0f} units")
                print(f"OVERSTOCK ALERT: {cat_data['overstock_units']:.0f} excess units")
                print(f"   → Implement clearance sales")
                print(f"   → Redistribute to high-demand stores")
                print(f"   → Adjust future procurement")
            
            # Good performance
            if (cat_data['inventory_days'] < avg_inventory_days * 0.9 and 
                cat_data['turnover_ratio'] > avg_turnover * 1.1):
                print(f"EXCELLENT PERFORMANCE")
                print(f"   → Maintain current strategy")
                print(f"   → Consider expanding product line")
                print(f"   → Use as benchmark for other categories")
        
        # Regional recommendations
        print("\n" + "="*60)
        print("REGIONAL OPTIMIZATION STRATEGIES")
        print("="*60)
        
        regional_performance = self.df.groupby('region').agg({
            'inventory_days': 'mean',
            'turnover_ratio': 'mean',
            'net_revenue': 'sum',
            'efficiency_score': 'mean'
        }).round(3)
        
        best_region = regional_performance['efficiency_score'].idxmax()
        worst_region = regional_performance['efficiency_score'].idxmin()
        
        print(f"\nBEST PERFORMING REGION: {best_region}")
        print(f"   Efficiency Score: {regional_performance.loc[best_region, 'efficiency_score']:.2f}")
        print(f"   → Share best practices with other regions")
        print(f"   → Consider expanding successful product lines")
        
        print(f"\nUNDERPERFORMING REGION: {worst_region}")
        print(f"   Efficiency Score: {regional_performance.loc[worst_region, 'efficiency_score']:.2f}")
        print(f"   → Implement targeted improvement plan")
        print(f"   → Review local market conditions")
        print(f"   → Consider staff training programs")
        
        # Seasonal recommendations
        print("\n" + "="*60)
        print("SEASONAL STRATEGY RECOMMENDATIONS")
        print("="*60)
        
        seasonal_perf = self.df.groupby('seasonality')['efficiency_score'].mean().sort_values(ascending=False)
        
        print("\nSEASONAL EFFICIENCY RANKING:")
        for season, score in seasonal_perf.items():
            print(f"   {season}: {score:.2f}")
        
        best_season = seasonal_perf.index[0]
        worst_season = seasonal_perf.index[-1]
        
        print(f"\nPEAK SEASON ({best_season}):")
        print(f"   - Increase inventory 2-3 weeks before peak")
        print(f"   - Prepare promotional campaigns")
        print(f"   - Ensure adequate staffing")
        
        print(f"\nLOW SEASON ({worst_season}):")
        print(f"   - Reduce inventory levels")
        print(f"   - Focus on clearance of slow-moving items")
        print(f"   - Plan maintenance and training activities")
        
        # Overall strategic priorities
        print("\n" + "="*60)
        print("TOP STRATEGIC PRIORITIES")
        print("="*60)
        
        priorities = [
            "1.  IMMEDIATE ACTIONS (0-30 days):",
            "   - Clear dead stock through aggressive discounting",
            "   - Redistribute overstock to high-demand locations",
            "   - Implement daily inventory monitoring",
            "",
            "2.  SHORT-TERM IMPROVEMENTS (1-3 months):",
            "   - Optimize reorder points based on turnover analysis",
            "   - Enhance demand forecasting accuracy",
            "   - Implement category-specific inventory strategies",
            "",
            "3.  LONG-TERM OPTIMIZATION (3-12 months):",
            "   - Develop predictive analytics for seasonal patterns",
            "   - Implement automated inventory management system",
            "   - Create regional inventory sharing network",
            "",
            "4.  INNOVATION OPPORTUNITIES:",
            "   - Test dynamic pricing based on inventory levels",
            "   - Explore drop-shipping for slow-moving categories",
            "   - Implement AI-driven demand forecasting"
        ]
        
        for priority in priorities:
            print(priority)
        
        # Save comprehensive analysis to file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        recommendations_file = f"/Users/vinayak/Desktop/Geetali/strategic_recommendations_{timestamp}.txt"
        
        with open(recommendations_file, 'w') as f:
            f.write("RETAIL INVENTORY STRATEGIC RECOMMENDATIONS\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write("By : Geetali Dutta\n\n")

            # Write current performance metrics
            f.write("CURRENT PERFORMANCE METRICS:\n")
            f.write(f"Average Inventory Days: {avg_inventory_days:.1f} days\n")
            f.write(f"Average Turnover Ratio: {avg_turnover:.3f}\n")
            f.write(f"Average Profit Margin: {avg_profit_margin:.1%}\n\n")
            
            # Write category-specific recommendations
            f.write("CATEGORY-SPECIFIC STRATEGIC RECOMMENDATIONS\n")
            f.write("=" * 60 + "\n\n")
            
            for category in category_performance.index:
                cat_data = category_performance.loc[category]
                f.write(f"{category.upper()} CATEGORY:\n")
                f.write("-" * 40 + "\n")
                
                # High inventory days
                if cat_data['inventory_days'] > avg_inventory_days * 1.2:
                    f.write(f"HIGH INVENTORY RISK: {cat_data['inventory_days']:.1f} days (vs avg {avg_inventory_days:.1f})\n")
                    f.write("   - Implement just-in-time ordering\n")
                    f.write("   - Review supplier lead times\n")
                    f.write("   - Consider promotional campaigns\n")
                
                # Low turnover
                if cat_data['turnover_ratio'] < avg_turnover * 0.8:
                    f.write(f"LOW TURNOVER: {cat_data['turnover_ratio']:.3f} (vs avg {avg_turnover:.3f})\n")
                    f.write("   - Optimize product mix\n")
                    f.write("   - Enhance marketing efforts\n")
                    f.write("   - Review pricing strategy\n")
                
                # High overstock
                if cat_data['overstock_units'] > self.df['overstock_units'].quantile(0.7):
                    f.write(f"OVERSTOCK ALERT: {cat_data['overstock_units']:.0f} excess units\n")
                    f.write("   - Implement clearance sales\n")
                    f.write("   - Redistribute to high-demand stores\n")
                    f.write("   - Adjust future procurement\n")
                
                # Good performance
                if (cat_data['inventory_days'] < avg_inventory_days * 0.9 and 
                    cat_data['turnover_ratio'] > avg_turnover * 1.1):
                    f.write("EXCELLENT PERFORMANCE\n")
                    f.write("   - Maintain current strategy\n")
                    f.write("   - Consider expanding product line\n")
                    f.write("   - Use as benchmark for other categories\n")
                
                f.write("\n")
            
            # Write regional recommendations
            f.write("REGIONAL OPTIMIZATION STRATEGIES\n")
            f.write("=" * 60 + "\n\n")
            
            f.write(f"BEST PERFORMING REGION: {best_region}\n")
            f.write(f"   Efficiency Score: {regional_performance.loc[best_region, 'efficiency_score']:.2f}\n")
            f.write("   - Share best practices with other regions\n")
            f.write("   - Consider expanding successful product lines\n\n")
            
            f.write(f"UNDERPERFORMING REGION: {worst_region}\n")
            f.write(f"   Efficiency Score: {regional_performance.loc[worst_region, 'efficiency_score']:.2f}\n")
            f.write("   - Implement targeted improvement plan\n")
            f.write("   - Review local market conditions\n")
            f.write("   - Consider staff training programs\n\n")
            
            # Write seasonal recommendations
            f.write("SEASONAL STRATEGY RECOMMENDATIONS\n")
            f.write("=" * 60 + "\n\n")
            
            f.write("SEASONAL EFFICIENCY RANKING:\n")
            for season, score in seasonal_perf.items():
                f.write(f"   {season}: {score:.2f}\n")
            
            f.write(f"\nPEAK SEASON ({best_season}):\n")
            f.write("   - Increase inventory 2-3 weeks before peak\n")
            f.write("   - Prepare promotional campaigns\n")
            f.write("   - Ensure adequate staffing\n\n")
            
            f.write(f"LOW SEASON ({worst_season}):\n")
            f.write("   - Reduce inventory levels\n")
            f.write("   - Focus on clearance of slow-moving items\n")
            f.write("   - Plan maintenance and training activities\n\n")
            
            # Write strategic priorities
            f.write("TOP STRATEGIC PRIORITIES\n")
            f.write("=" * 60 + "\n\n")
            for priority in priorities:
                f.write(priority + "\n")
            
            # Write key recommendations summary
            f.write("\nKEY RECOMMENDATIONS SUMMARY:\n")
            for i, rec in enumerate(recommendations, 1):
                f.write(f"{i}. {rec}\n")
        
        print(f"\nDetailed recommendations saved to: {recommendations_file}")
        
        return recommendations
    
    def generate_executive_summary(self):
        """Generate executive summary report"""
        if self.df is None:
            print("No data loaded. Please run load_data() first.")
            return
        
        print("\n" + "="*80)
        print("EXECUTIVE SUMMARY - RETAIL INVENTORY ANALYSIS")
        print("="*80)
        
        # Key metrics
        total_revenue = self.df['net_revenue'].sum()
        total_units = self.df['units_sold'].sum()
        avg_margin = self.df['profit_margin'].mean()
        total_overstock_value = (self.df['overstock_units'] * self.df['price']).sum()
        
        print(f"\n KEY BUSINESS METRICS:")
        print(f"   Total Net Revenue: ${total_revenue:,.2f}")
        print(f"   Total Units Sold: {total_units:,.0f}")
        print(f"   Average Profit Margin: {avg_margin:.1%}")
        print(f"   Overstock Value: ${total_overstock_value:,.2f}")
        
        # Performance by category
        top_categories = self.df.groupby('category')['net_revenue'].sum().sort_values(ascending=False).head(3)
        print(f"\n TOP PERFORMING CATEGORIES:")
        for i, (category, revenue) in enumerate(top_categories.items(), 1):
            print(f"   {i}. {category}: ${revenue:,.2f}")
        
        # Efficiency insights
        high_efficiency = len(self.df[self.df['efficiency_score'] > self.df['efficiency_score'].quantile(0.8)])
        low_efficiency = len(self.df[self.df['efficiency_score'] < self.df['efficiency_score'].quantile(0.2)])
        
        print(f"\n EFFICIENCY DISTRIBUTION:")
        print(f"   High Efficiency Products: {high_efficiency} ({high_efficiency/len(self.df)*100:.1f}%)")
        print(f"   Low Efficiency Products: {low_efficiency} ({low_efficiency/len(self.df)*100:.1f}%)")
        
        # ROI potential
        potential_savings = total_overstock_value * 0.3  # Assume 30% of overstock can be optimized
        print(f"\n OPTIMIZATION POTENTIAL:")
        print(f"   Estimated Savings from Overstock Reduction: ${potential_savings:,.2f}")
        print(f"   ROI from Inventory Optimization: {potential_savings/total_revenue*100:.1f}%")

def main():
    """Main execution function"""
    print(" Starting Retail Inventory Correlation Analysis...")
    
    # Initialize analytics engine
    analytics = RetailAnalytics()
    
    # Load and process data
    if not analytics.load_data():
        print("❌ Failed to load data. Exiting.")
        return
    
    # Calculate metrics
    analytics.calculate_inventory_metrics()
    
    # Perform analyses
    analytics.correlation_analysis()
    analytics.identify_problem_areas()
    analytics.seasonal_analysis()
    analytics.generate_strategic_recommendations()
    analytics.generate_executive_summary()
    
    print("\n Analysis completed successfully!")
    print(" Check the generated files for detailed recommendations.")           
    print(" By : Geetali Dutta")

if __name__ == "__main__":
    main()