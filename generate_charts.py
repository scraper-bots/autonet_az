import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# Set professional style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 14
plt.rcParams['axes.labelsize'] = 11

class AutonetBusinessAnalytics:
    def __init__(self, data_path='autonet_data.csv'):
        """Initialize and load data"""
        print("Loading data...")
        self.df = pd.read_csv(data_path, encoding='utf-8-sig')
        self.clean_data()
        print(f"Loaded {len(self.df):,} car listings")

    def clean_data(self):
        """Clean and prepare data for analysis"""
        # Convert date columns
        date_columns = ['Listed Date', 'Created At', 'Updated At']
        for col in date_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_datetime(self.df[col], errors='coerce')

        # Clean numeric columns
        numeric_columns = ['Price', 'Year', 'Engine Volume', 'Horsepower', 'Mileage']
        for col in numeric_columns:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        # Remove outliers for better visualization
        if 'Price' in self.df.columns:
            # Remove prices that are clearly errors (too low or too high)
            self.df = self.df[(self.df['Price'] >= 500) & (self.df['Price'] <= 500000)]

        if 'Year' in self.df.columns:
            # Keep reasonable years
            self.df = self.df[(self.df['Year'] >= 1980) & (self.df['Year'] <= 2025)]

        if 'Mileage' in self.df.columns:
            # Remove unrealistic mileage
            self.df = self.df[self.df['Mileage'] <= 1000000]

    def save_chart(self, filename):
        """Save chart with consistent formatting"""
        plt.tight_layout()
        plt.savefig(f'charts/{filename}', dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  ✓ Generated: charts/{filename}")

    def chart_1_top_car_makes(self):
        """Market Share: Top Car Brands by Inventory Volume"""
        print("\n1. Analyzing market share by brand...")

        top_makes = self.df['Car Make'].value_counts().head(15)

        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.barh(range(len(top_makes)), top_makes.values, color='#2E86AB')
        ax.set_yticks(range(len(top_makes)))
        ax.set_yticklabels(top_makes.index)
        ax.set_xlabel('Number of Listings')
        ax.set_title('Top 15 Car Brands by Inventory Volume', fontsize=16, fontweight='bold', pad=20)
        ax.invert_yaxis()

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars, top_makes.values)):
            ax.text(value + 50, i, f'{value:,}', va='center', fontweight='bold')

        ax.grid(axis='x', alpha=0.3)
        self.save_chart('01_top_car_makes.png')

    def chart_2_price_distribution(self):
        """Price Distribution: Understanding Market Price Ranges"""
        print("2. Analyzing price distribution...")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Histogram
        prices = self.df['Price'].dropna()
        ax1.hist(prices, bins=50, color='#A23B72', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Price (AZN)')
        ax1.set_ylabel('Number of Listings')
        ax1.set_title('Price Distribution Across All Listings', fontsize=14, fontweight='bold')
        ax1.axvline(prices.median(), color='red', linestyle='--', linewidth=2, label=f'Median: {prices.median():,.0f} AZN')
        ax1.axvline(prices.mean(), color='green', linestyle='--', linewidth=2, label=f'Average: {prices.mean():,.0f} AZN')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # Price ranges
        price_ranges = pd.cut(prices, bins=[0, 10000, 20000, 30000, 40000, 50000, 100000, 500000],
                              labels=['<10K', '10-20K', '20-30K', '30-40K', '40-50K', '50-100K', '100K+'])
        range_counts = price_ranges.value_counts().sort_index()

        ax2.bar(range(len(range_counts)), range_counts.values, color='#A23B72', edgecolor='black')
        ax2.set_xticks(range(len(range_counts)))
        ax2.set_xticklabels(range_counts.index, rotation=45)
        ax2.set_xlabel('Price Range (AZN)')
        ax2.set_ylabel('Number of Listings')
        ax2.set_title('Inventory Distribution by Price Bracket', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        # Add value labels
        for i, value in enumerate(range_counts.values):
            ax2.text(i, value + 30, f'{value:,}', ha='center', fontweight='bold')

        self.save_chart('02_price_distribution.png')

    def chart_3_average_price_by_brand(self):
        """Average Pricing: Which Brands Command Premium Prices"""
        print("3. Analyzing average prices by brand...")

        # Get top 15 brands by volume
        top_brands = self.df['Car Make'].value_counts().head(15).index
        brand_prices = self.df[self.df['Car Make'].isin(top_brands)].groupby('Car Make')['Price'].mean().sort_values(ascending=True)

        fig, ax = plt.subplots(figsize=(12, 8))
        colors = ['#06A77D' if x > brand_prices.median() else '#F18F01' for x in brand_prices.values]
        bars = ax.barh(range(len(brand_prices)), brand_prices.values, color=colors)
        ax.set_yticks(range(len(brand_prices)))
        ax.set_yticklabels(brand_prices.index)
        ax.set_xlabel('Average Price (AZN)')
        ax.set_title('Average Listing Price by Brand (Top 15 Brands)', fontsize=16, fontweight='bold', pad=20)

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars, brand_prices.values)):
            ax.text(value + 500, i, f'{value:,.0f} AZN', va='center', fontweight='bold')

        ax.axvline(brand_prices.median(), color='red', linestyle='--', alpha=0.5, linewidth=2, label='Median')
        ax.legend()
        ax.grid(axis='x', alpha=0.3)
        self.save_chart('03_average_price_by_brand.png')

    def chart_4_inventory_by_year(self):
        """Vehicle Age Analysis: Inventory Distribution by Manufacturing Year"""
        print("4. Analyzing inventory by vehicle year...")

        year_counts = self.df['Year'].value_counts().sort_index()
        recent_years = year_counts[year_counts.index >= 2000]

        fig, ax = plt.subplots(figsize=(14, 6))
        ax.bar(recent_years.index, recent_years.values, color='#C73E1D', edgecolor='black')
        ax.set_xlabel('Manufacturing Year')
        ax.set_ylabel('Number of Listings')
        ax.set_title('Inventory Volume by Vehicle Year (2000-2025)', fontsize=16, fontweight='bold', pad=20)
        ax.grid(axis='y', alpha=0.3)

        # Highlight peak year
        peak_year = recent_years.idxmax()
        peak_value = recent_years.max()
        ax.annotate(f'Peak: {peak_year}\n{peak_value:,} listings',
                   xy=(peak_year, peak_value),
                   xytext=(peak_year - 3, peak_value + 200),
                   arrowprops=dict(arrowstyle='->', color='red', lw=2),
                   fontsize=11, fontweight='bold', color='red')

        self.save_chart('04_inventory_by_year.png')

    def chart_5_geographic_distribution(self):
        """Geographic Distribution: Where Are the Listings Located"""
        print("5. Analyzing geographic distribution...")

        city_counts = self.df['City'].value_counts().head(15)

        fig, ax = plt.subplots(figsize=(12, 8))
        bars = ax.barh(range(len(city_counts)), city_counts.values, color='#6A4C93')
        ax.set_yticks(range(len(city_counts)))
        ax.set_yticklabels(city_counts.index)
        ax.set_xlabel('Number of Listings')
        ax.set_title('Top 15 Cities by Listing Volume', fontsize=16, fontweight='bold', pad=20)
        ax.invert_yaxis()

        # Add value labels and percentages
        total = len(self.df)
        for i, (bar, value) in enumerate(zip(bars, city_counts.values)):
            percentage = (value / total) * 100
            ax.text(value + 100, i, f'{value:,} ({percentage:.1f}%)', va='center', fontweight='bold')

        ax.grid(axis='x', alpha=0.3)
        self.save_chart('05_geographic_distribution.png')

    def chart_6_transmission_distribution(self):
        """Transmission Preference: Manual vs Automatic Market Split"""
        print("6. Analyzing transmission types...")

        # Map transmission codes
        transmission_map = {1: 'Automatic', 2: 'Manual'}
        trans_data = self.df['Transmission'].map(transmission_map).value_counts()

        fig, ax = plt.subplots(figsize=(10, 6))
        bars = ax.bar(trans_data.index, trans_data.values, color=['#2E86AB', '#F18F01'], edgecolor='black', width=0.5)
        ax.set_ylabel('Number of Listings')
        ax.set_title('Market Distribution: Automatic vs Manual Transmission', fontsize=16, fontweight='bold', pad=20)

        # Add value labels and percentages
        total = trans_data.sum()
        for bar, value in zip(bars, trans_data.values):
            height = bar.get_height()
            percentage = (value / total) * 100
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{value:,}\n({percentage:.1f}%)',
                   ha='center', va='bottom', fontweight='bold', fontsize=12)

        ax.grid(axis='y', alpha=0.3)
        self.save_chart('06_transmission_distribution.png')

    def chart_7_engine_type_distribution(self):
        """Fuel Type Analysis: Market Preference by Engine Type"""
        print("7. Analyzing engine types...")

        # Map engine types
        engine_map = {1: 'Gasoline', 2: 'Diesel', 3: 'Hybrid', 4: 'Electric', 5: 'Gas'}
        engine_data = self.df['Engine Type'].map(engine_map).value_counts()

        fig, ax = plt.subplots(figsize=(10, 7))
        colors = ['#06A77D', '#F18F01', '#2E86AB', '#A23B72', '#6A4C93']
        bars = ax.bar(range(len(engine_data)), engine_data.values, color=colors[:len(engine_data)], edgecolor='black')
        ax.set_xticks(range(len(engine_data)))
        ax.set_xticklabels(engine_data.index, rotation=45)
        ax.set_ylabel('Number of Listings')
        ax.set_title('Inventory by Fuel Type', fontsize=16, fontweight='bold', pad=20)

        # Add value labels and percentages
        total = engine_data.sum()
        for i, (bar, value) in enumerate(zip(bars, engine_data.values)):
            height = bar.get_height()
            percentage = (value / total) * 100
            ax.text(i, height + 100, f'{value:,}\n({percentage:.1f}%)',
                   ha='center', va='bottom', fontweight='bold')

        ax.grid(axis='y', alpha=0.3)
        self.save_chart('07_engine_type_distribution.png')

    def chart_8_mileage_analysis(self):
        """Mileage Distribution: Understanding Vehicle Usage Patterns"""
        print("8. Analyzing mileage patterns...")

        mileage = self.df['Mileage'].dropna()
        mileage_km = mileage[mileage <= 500000]  # Focus on reasonable mileage

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Histogram
        ax1.hist(mileage_km, bins=50, color='#C73E1D', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Mileage (km)')
        ax1.set_ylabel('Number of Listings')
        ax1.set_title('Mileage Distribution', fontsize=14, fontweight='bold')
        ax1.axvline(mileage_km.median(), color='blue', linestyle='--', linewidth=2,
                   label=f'Median: {mileage_km.median():,.0f} km')
        ax1.axvline(mileage_km.mean(), color='green', linestyle='--', linewidth=2,
                   label=f'Average: {mileage_km.mean():,.0f} km')
        ax1.legend()
        ax1.grid(axis='y', alpha=0.3)

        # Mileage categories
        mileage_ranges = pd.cut(mileage_km,
                               bins=[0, 50000, 100000, 150000, 200000, 300000, 500000],
                               labels=['<50K', '50-100K', '100-150K', '150-200K', '200-300K', '300K+'])
        range_counts = mileage_ranges.value_counts().sort_index()

        ax2.bar(range(len(range_counts)), range_counts.values, color='#C73E1D', edgecolor='black')
        ax2.set_xticks(range(len(range_counts)))
        ax2.set_xticklabels(range_counts.index, rotation=45)
        ax2.set_xlabel('Mileage Range (km)')
        ax2.set_ylabel('Number of Listings')
        ax2.set_title('Inventory by Mileage Bracket', fontsize=14, fontweight='bold')

        # Add value labels
        for i, value in enumerate(range_counts.values):
            ax2.text(i, value + 30, f'{value:,}', ha='center', fontweight='bold')

        ax2.grid(axis='y', alpha=0.3)
        self.save_chart('08_mileage_analysis.png')

    def chart_9_credit_barter_analysis(self):
        """Payment Options: Credit and Barter Availability"""
        print("9. Analyzing payment options...")

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        # Credit availability
        credit_map = {1: 'Cash Only', 2: 'Credit Available'}
        credit_data = self.df['Credit Available'].map(credit_map).value_counts()

        bars1 = ax1.bar(credit_data.index, credit_data.values, color=['#F18F01', '#06A77D'], edgecolor='black')
        ax1.set_ylabel('Number of Listings')
        ax1.set_title('Credit Financing Availability', fontsize=14, fontweight='bold')

        total = credit_data.sum()
        for bar, value in zip(bars1, credit_data.values):
            height = bar.get_height()
            percentage = (value / total) * 100
            ax1.text(bar.get_x() + bar.get_width()/2., height,
                    f'{value:,}\n({percentage:.1f}%)',
                    ha='center', va='bottom', fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)

        # Barter availability
        barter_map = {1: 'No Barter', 2: 'Barter Accepted'}
        barter_data = self.df['Barter Available'].map(barter_map).value_counts()

        bars2 = ax2.bar(barter_data.index, barter_data.values, color=['#F18F01', '#2E86AB'], edgecolor='black')
        ax2.set_ylabel('Number of Listings')
        ax2.set_title('Barter/Trade Acceptance', fontsize=14, fontweight='bold')

        for bar, value in zip(bars2, barter_data.values):
            height = bar.get_height()
            percentage = (value / total) * 100
            ax2.text(bar.get_x() + bar.get_width()/2., height,
                    f'{value:,}\n({percentage:.1f}%)',
                    ha='center', va='bottom', fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        self.save_chart('09_credit_barter_analysis.png')

    def chart_10_top_models_by_brand(self):
        """Popular Models: Best-Selling Models Within Top Brands"""
        print("10. Analyzing top models within popular brands...")

        # Get top 5 brands
        top_5_brands = self.df['Car Make'].value_counts().head(5).index

        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()

        for idx, brand in enumerate(top_5_brands):
            brand_data = self.df[self.df['Car Make'] == brand]
            top_models = brand_data['Car Model'].value_counts().head(8)

            ax = axes[idx]
            bars = ax.barh(range(len(top_models)), top_models.values, color='#6A4C93')
            ax.set_yticks(range(len(top_models)))
            ax.set_yticklabels(top_models.index)
            ax.set_xlabel('Number of Listings')
            ax.set_title(f'{brand} - Top Models', fontsize=12, fontweight='bold')
            ax.invert_yaxis()

            # Add value labels
            for i, (bar, value) in enumerate(zip(bars, top_models.values)):
                ax.text(value + 5, i, f'{value:,}', va='center', fontsize=9, fontweight='bold')

            ax.grid(axis='x', alpha=0.3)

        # Remove empty subplot
        fig.delaxes(axes[5])

        plt.suptitle('Most Popular Models by Brand (Top 5 Brands)', fontsize=16, fontweight='bold', y=1.00)
        self.save_chart('10_top_models_by_brand.png')

    def chart_11_price_vs_year(self):
        """Price Depreciation: How Vehicle Age Affects Market Value"""
        print("11. Analyzing price trends by vehicle age...")

        # Group by year and calculate average price
        recent_data = self.df[self.df['Year'] >= 2005].copy()
        year_prices = recent_data.groupby('Year')['Price'].agg(['mean', 'median', 'count'])
        year_prices = year_prices[year_prices['count'] >= 20]  # Filter years with enough data

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

        # Line chart for price trends
        ax1.plot(year_prices.index, year_prices['mean'], marker='o', linewidth=2,
                markersize=6, color='#2E86AB', label='Average Price')
        ax1.plot(year_prices.index, year_prices['median'], marker='s', linewidth=2,
                markersize=6, color='#F18F01', label='Median Price')
        ax1.set_xlabel('Manufacturing Year')
        ax1.set_ylabel('Price (AZN)')
        ax1.set_title('Average Vehicle Price by Year', fontsize=14, fontweight='bold')
        ax1.legend()
        ax1.grid(alpha=0.3)

        # Calculate vehicle age and price relationship
        current_year = 2025
        recent_data['Age'] = current_year - recent_data['Year']
        age_price = recent_data[recent_data['Age'] <= 20].groupby('Age')['Price'].mean()

        ax2.bar(age_price.index, age_price.values, color='#A23B72', edgecolor='black')
        ax2.set_xlabel('Vehicle Age (years)')
        ax2.set_ylabel('Average Price (AZN)')
        ax2.set_title('Price Depreciation by Vehicle Age', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)

        self.save_chart('11_price_vs_year.png')

    def chart_12_features_analysis(self):
        """Feature Richness: Distribution of Additional Features"""
        print("12. Analyzing feature counts...")

        features = self.df['Features Count'].value_counts().sort_index()
        features = features[features.index <= 15]  # Focus on reasonable feature counts

        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(features.index, features.values, color='#06A77D', edgecolor='black')
        ax.set_xlabel('Number of Additional Features')
        ax.set_ylabel('Number of Listings')
        ax.set_title('Inventory Distribution by Feature Count', fontsize=16, fontweight='bold', pad=20)

        # Add value labels
        for bar, value in zip(bars, features.values):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 20,
                   f'{value:,}', ha='center', va='bottom', fontweight='bold', fontsize=9)

        # Add average line
        avg_features = self.df['Features Count'].mean()
        ax.axvline(avg_features, color='red', linestyle='--', linewidth=2,
                  label=f'Average: {avg_features:.1f} features')
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        self.save_chart('12_features_analysis.png')

    def chart_13_price_vs_mileage(self):
        """Price vs Mileage: Understanding Value Proposition"""
        print("13. Analyzing price-mileage relationship...")

        # Filter reasonable data
        clean_data = self.df[(self.df['Price'] >= 1000) &
                            (self.df['Price'] <= 100000) &
                            (self.df['Mileage'] <= 400000)].copy()

        # Create mileage bins
        clean_data['Mileage_Range'] = pd.cut(clean_data['Mileage'],
                                            bins=[0, 50000, 100000, 150000, 200000, 300000, 400000],
                                            labels=['0-50K', '50-100K', '100-150K', '150-200K', '200-300K', '300-400K'])

        mileage_price = clean_data.groupby('Mileage_Range')['Price'].mean()

        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(range(len(mileage_price)), mileage_price.values, color='#C73E1D', edgecolor='black')
        ax.set_xticks(range(len(mileage_price)))
        ax.set_xticklabels(mileage_price.index, rotation=45)
        ax.set_xlabel('Mileage Range (km)')
        ax.set_ylabel('Average Price (AZN)')
        ax.set_title('Average Vehicle Price by Mileage Range', fontsize=16, fontweight='bold', pad=20)

        # Add value labels
        for i, (bar, value) in enumerate(zip(bars, mileage_price.values)):
            ax.text(i, value + 300, f'{value:,.0f} AZN', ha='center', fontweight='bold')

        ax.grid(axis='y', alpha=0.3)
        self.save_chart('13_price_vs_mileage.png')

    def chart_14_listing_timeline(self):
        """Market Activity: Listing Volume Over Time"""
        print("14. Analyzing listing timeline...")

        # Extract month-year from listing date
        self.df['Listing_Month'] = self.df['Listed Date'].dt.to_period('M')
        timeline = self.df['Listing_Month'].value_counts().sort_index().tail(12)

        fig, ax = plt.subplots(figsize=(14, 6))
        ax.plot(range(len(timeline)), timeline.values, marker='o', linewidth=2.5,
               markersize=8, color='#2E86AB')
        ax.fill_between(range(len(timeline)), timeline.values, alpha=0.3, color='#2E86AB')
        ax.set_xticks(range(len(timeline)))
        ax.set_xticklabels([str(x) for x in timeline.index], rotation=45)
        ax.set_xlabel('Month')
        ax.set_ylabel('Number of New Listings')
        ax.set_title('Market Activity: New Listings Per Month (Last 12 Months)', fontsize=16, fontweight='bold', pad=20)
        ax.grid(alpha=0.3)

        self.save_chart('14_listing_timeline.png')

    def chart_15_price_by_transmission_and_fuel(self):
        """Price Analysis: Transmission & Fuel Type Impact on Pricing"""
        print("15. Analyzing price by transmission and fuel type...")

        # Map values
        transmission_map = {1: 'Automatic', 2: 'Manual'}
        engine_map = {1: 'Gasoline', 2: 'Diesel', 3: 'Hybrid'}

        self.df['Trans_Type'] = self.df['Transmission'].map(transmission_map)
        self.df['Fuel_Type'] = self.df['Engine Type'].map(engine_map)

        # Calculate average prices
        trans_fuel_price = self.df.groupby(['Trans_Type', 'Fuel_Type'])['Price'].mean().reset_index()
        trans_fuel_price = trans_fuel_price.dropna()

        fig, ax = plt.subplots(figsize=(12, 6))

        # Create grouped bar chart
        fuel_types = trans_fuel_price['Fuel_Type'].unique()
        x = np.arange(len(fuel_types))
        width = 0.35

        auto_prices = []
        manual_prices = []

        for fuel in fuel_types:
            auto_price = trans_fuel_price[(trans_fuel_price['Trans_Type'] == 'Automatic') &
                                         (trans_fuel_price['Fuel_Type'] == fuel)]['Price'].values
            manual_price = trans_fuel_price[(trans_fuel_price['Trans_Type'] == 'Manual') &
                                           (trans_fuel_price['Fuel_Type'] == fuel)]['Price'].values

            auto_prices.append(auto_price[0] if len(auto_price) > 0 else 0)
            manual_prices.append(manual_price[0] if len(manual_price) > 0 else 0)

        ax.bar(x - width/2, auto_prices, width, label='Automatic', color='#2E86AB', edgecolor='black')
        ax.bar(x + width/2, manual_prices, width, label='Manual', color='#F18F01', edgecolor='black')

        ax.set_xlabel('Fuel Type')
        ax.set_ylabel('Average Price (AZN)')
        ax.set_title('Average Price by Transmission and Fuel Type', fontsize=16, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels(fuel_types)
        ax.legend()
        ax.grid(axis='y', alpha=0.3)

        self.save_chart('15_price_by_transmission_fuel.png')

    def generate_all_charts(self):
        """Generate all business analytics charts"""
        print("\n" + "="*60)
        print("AUTONET.AZ - BUSINESS ANALYTICS DASHBOARD")
        print("Generating comprehensive market insights...")
        print("="*60)

        self.chart_1_top_car_makes()
        self.chart_2_price_distribution()
        self.chart_3_average_price_by_brand()
        self.chart_4_inventory_by_year()
        self.chart_5_geographic_distribution()
        self.chart_6_transmission_distribution()
        self.chart_7_engine_type_distribution()
        self.chart_8_mileage_analysis()
        self.chart_9_credit_barter_analysis()
        self.chart_10_top_models_by_brand()
        self.chart_11_price_vs_year()
        self.chart_12_features_analysis()
        self.chart_13_price_vs_mileage()
        self.chart_14_listing_timeline()
        self.chart_15_price_by_transmission_and_fuel()

        print("\n" + "="*60)
        print("ANALYSIS COMPLETE!")
        print(f"Generated 15 business insight charts in 'charts/' directory")
        print("="*60 + "\n")

def main():
    """Main execution function"""
    try:
        analytics = AutonetBusinessAnalytics('autonet_data.csv')
        analytics.generate_all_charts()
        print("\nAll visualizations have been successfully generated!")
        print("Charts are ready for business presentation.\n")

    except FileNotFoundError:
        print("Error: autonet_data.csv not found in the current directory")
        print("Please ensure the data file is in the same folder as this script")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
