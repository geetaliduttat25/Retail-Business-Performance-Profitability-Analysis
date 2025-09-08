-- =====================================================
-- RETAIL BUSINESS PERFORMANCE & PROFITABILITY ANALYSIS
-- SQL Queries for Profit Margin Analysis
-- By : Geetali Dutta 
-- =====================================================

USE retail_analysis;

-- 1. PROFIT MARGINS BY CATEGORY AND SUB-CATEGORY
-- =====================================================

-- Overall profit margins by category
SELECT 
    category,
    COUNT(*) as total_transactions,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(units_sold * price), 2) as gross_revenue,
    ROUND(SUM(units_sold * price * discount / 100), 2) as total_discount_amount,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount_percent,
    ROUND((SUM(units_sold * price * (1 - discount / 100.0)) / SUM(units_sold * price)) * 100, 2) as profit_margin_percent
FROM retail_inventory 
WHERE units_sold > 0
GROUP BY category
ORDER BY profit_margin_percent DESC;

-- Profit margins by category and region (sub-category analysis)
SELECT 
    category,
    region,
    COUNT(*) as transactions,
    SUM(units_sold) as units_sold,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount,
    ROUND((SUM(units_sold * price * (1 - discount / 100.0)) / SUM(units_sold * price)) * 100, 2) as profit_margin_percent
FROM retail_inventory 
WHERE units_sold > 0
GROUP BY category, region
ORDER BY category, profit_margin_percent DESC;

-- 2. INVENTORY TURNOVER ANALYSIS
-- =====================================================

-- Inventory turnover by category with profitability correlation
SELECT 
    category,
    COUNT(*) as total_records,
    ROUND(AVG(inventory_level), 2) as avg_inventory_level,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(units_sold) / AVG(inventory_level), 2) as inventory_turnover_ratio,
    ROUND(AVG(CASE WHEN inventory_level > units_sold THEN inventory_level - units_sold ELSE 0 END), 2) as avg_overstock,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue,
    ROUND((SUM(units_sold * price * (1 - discount / 100.0)) / SUM(units_sold * price)) * 100, 2) as profit_margin_percent
FROM retail_inventory
GROUP BY category
ORDER BY inventory_turnover_ratio DESC;

-- Store-wise inventory performance
SELECT 
    store_id,
    category,
    ROUND(AVG(inventory_level), 2) as avg_inventory,
    SUM(units_sold) as total_sold,
    ROUND(SUM(units_sold) / AVG(inventory_level), 2) as turnover_ratio,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue
FROM retail_inventory
GROUP BY store_id, category
HAVING AVG(inventory_level) > 0
ORDER BY store_id, turnover_ratio DESC;

-- 3. SEASONAL PRODUCT BEHAVIOR ANALYSIS
-- =====================================================

-- Seasonal performance by category
SELECT 
    seasonality,
    category,
    COUNT(*) as transactions,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount,
    ROUND(AVG(demand_forecast), 2) as avg_demand_forecast,
    ROUND(AVG(units_sold / NULLIF(demand_forecast, 0)) * 100, 2) as demand_fulfillment_percent
FROM retail_inventory
WHERE units_sold > 0 AND demand_forecast > 0
GROUP BY seasonality, category
ORDER BY seasonality, net_revenue DESC;

-- Seasonal trends with weather impact
SELECT 
    seasonality,
    weather_condition,
    COUNT(*) as transactions,
    SUM(units_sold) as total_units_sold,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as net_revenue
FROM retail_inventory
WHERE units_sold > 0
GROUP BY seasonality, weather_condition
ORDER BY seasonality, net_revenue DESC;

-- 4. SLOW-MOVING AND OVERSTOCKED ITEMS IDENTIFICATION
-- =====================================================

-- Products with high inventory but low sales (slow-moving)
SELECT 
    product_id,
    category,
    store_id,
    region,
    ROUND(AVG(inventory_level), 2) as avg_inventory,
    ROUND(AVG(units_sold), 2) as avg_units_sold,
    ROUND(AVG(inventory_level) - AVG(units_sold), 2) as avg_overstock,
    ROUND(AVG(units_sold) / NULLIF(AVG(inventory_level), 0) * 100, 2) as sell_through_rate,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount
FROM retail_inventory
GROUP BY product_id, category, store_id, region
HAVING AVG(inventory_level) > AVG(units_sold) * 2  -- Inventory is more than 2x sales
   AND AVG(units_sold) / NULLIF(AVG(inventory_level), 0) < 0.3  -- Less than 30% sell-through
ORDER BY avg_overstock DESC
LIMIT 20;

-- Category-wise overstock analysis
SELECT 
    category,
    region,
    COUNT(*) as product_count,
    ROUND(AVG(inventory_level - units_sold), 2) as avg_overstock_per_transaction,
    ROUND(SUM(CASE WHEN inventory_level > units_sold * 2 THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as overstock_percentage,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(SUM((inventory_level - units_sold) * price), 2) as tied_up_capital
FROM retail_inventory
WHERE inventory_level > units_sold
GROUP BY category, region
ORDER BY tied_up_capital DESC;

-- 5. DEMAND FORECASTING ACCURACY ANALYSIS
-- =====================================================

-- Forecast vs actual sales analysis
SELECT 
    category,
    region,
    COUNT(*) as transactions,
    ROUND(AVG(demand_forecast), 2) as avg_forecasted_demand,
    ROUND(AVG(units_sold), 2) as avg_actual_sales,
    ROUND(AVG(units_sold - demand_forecast), 2) as avg_forecast_error,
    ROUND(AVG(ABS(units_sold - demand_forecast)), 2) as avg_absolute_error,
    ROUND(AVG(ABS(units_sold - demand_forecast) / NULLIF(demand_forecast, 0)) * 100, 2) as avg_percentage_error,
    ROUND(SUM(CASE WHEN units_sold > demand_forecast THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as underforecast_percentage
FROM retail_inventory
WHERE demand_forecast > 0
GROUP BY category, region
ORDER BY avg_percentage_error ASC;

-- 6. PROMOTIONAL EFFECTIVENESS ANALYSIS
-- =====================================================

-- Holiday/Promotion impact on sales and profitability
SELECT 
    CASE WHEN holiday_promotion = 1 THEN 'With Promotion' ELSE 'No Promotion' END as promotion_status,
    category,
    COUNT(*) as transactions,
    ROUND(AVG(units_sold), 2) as avg_units_sold,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as total_net_revenue,
    ROUND(AVG(units_sold * price * (1 - discount / 100.0)), 2) as avg_net_revenue_per_transaction
FROM retail_inventory
WHERE units_sold > 0
GROUP BY holiday_promotion, category
ORDER BY category, promotion_status;

-- Discount effectiveness by category
SELECT 
    category,
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount <= 10 THEN 'Low (1-10%)'
        WHEN discount <= 20 THEN 'Medium (11-20%)'
        ELSE 'High (>20%)'
    END as discount_tier,
    COUNT(*) as transactions,
    ROUND(AVG(units_sold), 2) as avg_units_sold,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as total_net_revenue,
    ROUND(AVG(units_sold * price * (1 - discount / 100.0)), 2) as avg_revenue_per_transaction
FROM retail_inventory
WHERE units_sold > 0
GROUP BY category, discount_tier
ORDER BY category, discount_tier;

-- 7. COMPETITOR PRICING ANALYSIS
-- =====================================================

-- Price competitiveness analysis
SELECT 
    category,
    region,
    COUNT(*) as transactions,
    ROUND(AVG(price), 2) as avg_our_price,
    ROUND(AVG(competitor_pricing), 2) as avg_competitor_price,
    ROUND(AVG(price - competitor_pricing), 2) as avg_price_difference,
    ROUND(AVG((price - competitor_pricing) / NULLIF(competitor_pricing, 0)) * 100, 2) as avg_price_premium_percent,
    ROUND(AVG(units_sold), 2) as avg_units_sold,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as total_net_revenue
FROM retail_inventory
WHERE competitor_pricing > 0 AND units_sold > 0
GROUP BY category, region
ORDER BY avg_price_premium_percent DESC;

-- 8. TOP PERFORMING AND UNDERPERFORMING PRODUCTS
-- =====================================================

-- Top 10 most profitable products
SELECT 
    product_id,
    category,
    store_id,
    region,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as total_net_revenue,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount,
    ROUND(AVG(inventory_level), 2) as avg_inventory,
    ROUND(SUM(units_sold) / AVG(inventory_level), 2) as turnover_ratio
FROM retail_inventory
WHERE units_sold > 0
GROUP BY product_id, category, store_id, region
ORDER BY total_net_revenue DESC
LIMIT 10;

-- Bottom 10 underperforming products (by revenue)
SELECT 
    product_id,
    category,
    store_id,
    region,
    SUM(units_sold) as total_units_sold,
    ROUND(SUM(units_sold * price * (1 - discount / 100.0)), 2) as total_net_revenue,
    ROUND(AVG(price), 2) as avg_price,
    ROUND(AVG(discount), 2) as avg_discount,
    ROUND(AVG(inventory_level), 2) as avg_inventory,
    ROUND(AVG(inventory_level - units_sold), 2) as avg_overstock
FROM retail_inventory
WHERE units_sold >= 0
GROUP BY product_id, category, store_id, region
ORDER BY total_net_revenue ASC
LIMIT 10;

-- 9. SUMMARY DASHBOARD QUERIES
-- =====================================================

-- Executive summary metrics
SELECT 
    'Total Revenue' as metric,
    CONCAT('$', FORMAT(SUM(units_sold * price), 2)) as value
FROM retail_inventory
WHERE units_sold > 0
UNION ALL
SELECT 
    'Net Revenue (After Discounts)' as metric,
    CONCAT('$', FORMAT(SUM(units_sold * price * (1 - discount / 100.0)), 2)) as value
FROM retail_inventory
WHERE units_sold > 0
UNION ALL
SELECT 
    'Total Units Sold' as metric,
    FORMAT(SUM(units_sold), 0) as value
FROM retail_inventory
WHERE units_sold > 0
UNION ALL
SELECT 
    'Average Profit Margin' as metric,
    CONCAT(ROUND(AVG((units_sold * price * (1 - discount / 100.0)) / (units_sold * price) * 100), 2), '%') as value
FROM retail_inventory
WHERE units_sold > 0 AND price > 0
UNION ALL
SELECT 
    'Average Inventory Turnover' as metric,
    ROUND(AVG(units_sold / NULLIF(inventory_level, 0)), 2) as value
FROM retail_inventory
WHERE inventory_level > 0;

-- =====================================================
-- END OF PROFIT ANALYSIS QUERIES
-- Intership project of Geetali Dutta 
-- =====================================================