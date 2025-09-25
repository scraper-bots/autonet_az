import asyncio
import aiohttp
import pandas as pd
import json
import time
from typing import List, Dict, Any
import sys
from tqdm import tqdm

class FastAutonetScraper:
    def __init__(self, concurrent_requests=50):
        self.base_url = "https://autonet.az/api/items/searchItem/"
        self.headers = {
            'accept': 'application/json',
            'accept-encoding': 'gzip, deflate, br, zstd',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8,ru;q=0.7,az;q=0.6',
            'authorization': 'Bearer null',
            'connection': 'keep-alive',
            'dnt': '1',
            'host': 'autonet.az',
            'origin': 'https://www.autonet.az',
            'referer': 'https://www.autonet.az/',
            'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
            'x-authorization': '00028c2ddcc1ca6c32bc919dca64c288bf32ff2a',
            'x-requested-with': 'XMLHttpRequest'
        }
        self.concurrent_requests = concurrent_requests
        self.all_data = []

    async def fetch_page(self, session: aiohttp.ClientSession, page: int, semaphore: asyncio.Semaphore) -> tuple[int, Dict[str, Any]]:
        """Fetch a single page of data"""
        async with semaphore:
            try:
                params = {'page': page}
                async with session.get(self.base_url, params=params, headers=self.headers, timeout=aiohttp.ClientTimeout(total=30)) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return page, data
            except Exception as e:
                print(f"Error fetching page {page}: {e}")
                return page, None

    async def scrape_all_pages(self) -> List[Dict[str, Any]]:
        """Scrape all pages of data using async requests"""
        print("Starting to scrape autonet.az data with async requests...")
        start_time = time.time()

        # Create semaphore to limit concurrent requests
        semaphore = asyncio.Semaphore(self.concurrent_requests)

        async with aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=self.concurrent_requests),
            timeout=aiohttp.ClientTimeout(total=60)
        ) as session:
            # Get first page to understand total pages
            _, first_page_data = await self.fetch_page(session, 1, semaphore)
            if not first_page_data:
                print("Failed to fetch first page")
                return []

            total_pages = first_page_data.get('last_page', 0)
            total_items = first_page_data.get('total', 0)

            print(f"Total pages: {total_pages}")
            print(f"Total items: {total_items}")
            print(f"Using {self.concurrent_requests} concurrent requests")

            # Create tasks for all pages
            tasks = []
            for page in range(1, total_pages + 1):
                task = self.fetch_page(session, page, semaphore)
                tasks.append(task)

            # Execute all tasks with progress bar
            results = []
            failed_pages = []

            with tqdm(total=len(tasks), desc="Scraping pages", unit="page") as pbar:
                for coro in asyncio.as_completed(tasks):
                    page_num, page_data = await coro
                    if page_data and 'data' in page_data:
                        results.extend(page_data['data'])
                        pbar.set_postfix({"Items": len(results), "Failed": len(failed_pages)})
                    else:
                        failed_pages.append(page_num)
                    pbar.update(1)

            self.all_data = results

            # Retry failed pages if any
            if failed_pages:
                print(f"\nRetrying {len(failed_pages)} failed pages...")
                retry_semaphore = asyncio.Semaphore(10)  # Lower concurrency for retries

                retry_tasks = [self.fetch_page(session, page, retry_semaphore) for page in failed_pages]

                for task in asyncio.as_completed(retry_tasks):
                    page_num, page_data = await task
                    if page_data and 'data' in page_data:
                        self.all_data.extend(page_data['data'])
                        print(f"✓ Retry successful for page {page_num}")
                    else:
                        print(f"✗ Retry failed for page {page_num}")

        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"\nScraping completed in {elapsed_time:.2f} seconds!")
        print(f"Total items collected: {len(self.all_data)}")
        print(f"Average speed: {len(self.all_data)/elapsed_time:.1f} items/second")

        return self.all_data

    def clean_and_normalize_data(self) -> pd.DataFrame:
        """Clean and normalize the scraped data"""
        if not self.all_data:
            print("No data to process")
            return pd.DataFrame()

        print("Cleaning and normalizing data...")

        # Convert to DataFrame
        df = pd.DataFrame(self.all_data)

        # Parse elavemelumat field (additional features)
        if 'elavemelumat' in df.columns:
            df['elavemelumat_parsed'] = df['elavemelumat'].apply(
                lambda x: json.loads(x) if x and isinstance(x, str) and x != 'null' else []
            )
            df['elavemelumat_count'] = df['elavemelumat_parsed'].apply(len)

        # Convert date columns
        date_columns = ['date', 'created_at', 'updated_at']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Clean numeric columns
        numeric_columns = ['price', 'buraxilis_ili', 'muherrikin_hecmi', 'at_gucu', 'yurus', 'engine_capacity']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Create readable column names mapping
        column_mapping = {
            'id': 'ID',
            'cover': 'Cover Image',
            'price': 'Price',
            'information': 'Description',
            'fullname': 'Seller Name',
            'phone1': 'Phone 1',
            'phone2': 'Phone 2',
            'make': 'Car Make',
            'model': 'Car Model',
            'buraxilis_ili': 'Year',
            'muherrikin_tipi': 'Engine Type',
            'muherrikin_hecmi': 'Engine Volume',
            'at_gucu': 'Horsepower',
            'suret_qutusu': 'Transmission',
            'rengi': 'Color',
            'yurus': 'Mileage',
            'region': 'Region',
            'cityName': 'City',
            'currency': 'Currency',
            'kredit': 'Credit Available',
            'barter': 'Barter Available',
            'date': 'Listed Date',
            'created_at': 'Created At',
            'updated_at': 'Updated At',
            'elavemelumat_count': 'Features Count'
        }

        # Rename columns for better readability
        df_renamed = df.rename(columns=column_mapping)

        print(f"Data processed: {len(df_renamed)} rows, {len(df_renamed.columns)} columns")
        return df_renamed

    def save_to_csv(self, df: pd.DataFrame, filename: str = 'autonet_data.csv'):
        """Save data to CSV file"""
        try:
            df.to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving CSV: {e}")

    def save_to_xlsx(self, df: pd.DataFrame, filename: str = 'autonet_data.xlsx'):
        """Save data to XLSX file"""
        try:
            with pd.ExcelWriter(filename, engine='openpyxl') as writer:
                # Main data sheet
                df.to_excel(writer, sheet_name='Car Listings', index=False)

                # Summary statistics sheet
                summary_stats = self.create_summary_stats(df)
                summary_stats.to_excel(writer, sheet_name='Summary Stats', index=True)

                # Top makes sheet
                if 'Car Make' in df.columns:
                    top_makes = df['Car Make'].value_counts().head(20).reset_index()
                    top_makes.columns = ['Car Make', 'Count']
                    top_makes.to_excel(writer, sheet_name='Top Makes', index=False)

                # Price ranges sheet
                if 'Price' in df.columns:
                    price_ranges = pd.cut(df['Price'], bins=10).value_counts().reset_index()
                    price_ranges.columns = ['Price Range', 'Count']
                    price_ranges.to_excel(writer, sheet_name='Price Distribution', index=False)

            print(f"Data saved to {filename}")
        except Exception as e:
            print(f"Error saving XLSX: {e}")

    def create_summary_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create summary statistics"""
        stats = {}

        if 'Price' in df.columns:
            stats['Total Listings'] = len(df)
            stats['Average Price (AZN)'] = df['Price'].mean()
            stats['Median Price (AZN)'] = df['Price'].median()
            stats['Min Price (AZN)'] = df['Price'].min()
            stats['Max Price (AZN)'] = df['Price'].max()

        if 'Car Make' in df.columns:
            stats['Unique Car Makes'] = df['Car Make'].nunique()
            stats['Most Popular Make'] = df['Car Make'].mode().iloc[0] if not df['Car Make'].mode().empty else 'N/A'

        if 'Year' in df.columns:
            stats['Average Year'] = df['Year'].mean()
            stats['Oldest Car Year'] = df['Year'].min()
            stats['Newest Car Year'] = df['Year'].max()

        if 'Mileage' in df.columns:
            stats['Average Mileage (km)'] = df['Mileage'].mean()
            stats['Median Mileage (km)'] = df['Mileage'].median()

        return pd.DataFrame(list(stats.items()), columns=['Metric', 'Value'])

async def main():
    """Main async function to run the scraper"""
    scraper = FastAutonetScraper(concurrent_requests=50)  # Adjust based on your needs

    try:
        # Scrape all data
        data = await scraper.scrape_all_pages()

        if not data:
            print("No data was scraped. Exiting.")
            return

        # Clean and normalize data
        df = scraper.clean_and_normalize_data()

        if df.empty:
            print("No data to save. Exiting.")
            return

        # Save to files
        scraper.save_to_csv(df)
        scraper.save_to_xlsx(df)

        print("\n=== SCRAPING COMPLETED SUCCESSFULLY ===")
        print(f"Total items scraped: {len(df)}")
        print("Files created:")
        print("- autonet_data.csv")
        print("- autonet_data.xlsx (with multiple sheets)")

    except KeyboardInterrupt:
        print("\nScraping interrupted by user")
        if scraper.all_data:
            print(f"Saving {len(scraper.all_data)} items collected so far...")
            df = scraper.clean_and_normalize_data()
            scraper.save_to_csv(df, 'autonet_data_partial.csv')
            scraper.save_to_xlsx(df, 'autonet_data_partial.xlsx')
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())