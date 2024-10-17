import oracledb
from queue import Queue
from typing import List, Dict
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor

@dataclass
class DBConfig:
    username: str
    password: str
    host: str
    port: int
    service_name: str
    
    def get_dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"

class OracleYearPartitionedQuery:
    def __init__(self, db_config: DBConfig, start_year: int = 2006, end_year: int = 2023, 
                 thick_mode: bool = False):
        self.db_config = db_config
        self.start_year = start_year
        self.end_year = end_year
        self.result_queue = Queue()
        
        if thick_mode:
            oracledb.init_oracle_client()
    
    def get_connection(self):
        return oracledb.connect(
            user=self.db_config.username,
            password=self.db_config.password,
            dsn=self.db_config.get_dsn()
        )
    
    def process_year(self, base_query: str, year: int) -> List[Dict]:
        """Process a specific year of data"""
        wrapped_query = f"""
            WITH filtered_data AS (
                {base_query}
            )
            SELECT *
            FROM filtered_data
            WHERE YR = {year}
        """
        
        results = []
        try:
            with self.get_connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(wrapped_query)
                    columns = [col[0] for col in cursor.description]
                    
                    # Fetch in batches to handle large result sets
                    batch_size = 10000
                    total_rows = 0
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                        batch_results = [dict(zip(columns, row)) for row in rows]
                        results.extend(batch_results)
                        total_rows += len(batch_results)
                        print(f"Year {year}: Processed {total_rows} rows so far...")
                            
            print(f"Completed Year {year}: Total {len(results)} records")
            
        except Exception as e:
            print(f"Error processing year {year}: {str(e)}")
            raise
        
        self.result_queue.put((year, results))
        return results

    def execute_parallel(self, query: str, max_concurrent_threads: int = 4) -> List[Dict]:
        """Execute the query in parallel using year-based partitioning"""
        years = list(range(self.start_year, self.end_year + 1))
        total_years = len(years)
        
        print(f"Starting parallel execution for years {self.start_year} to {self.end_year}")
        print(f"Using maximum {max_concurrent_threads} concurrent threads")
        
        start_time = time.time()
        
        # Execute queries in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=max_concurrent_threads) as executor:
            futures = [
                executor.submit(self.process_year, query, year)
                for year in years
            ]
            
            # Wait for all futures to complete
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    print(f"Thread execution failed: {str(e)}")
        
        # Combine all results in chronological order
        all_results = []
        temp_results = []
        
        while not self.result_queue.empty():
            temp_results.append(self.result_queue.get())
        
        # Sort by year and extract results
        temp_results.sort(key=lambda x: x[0])  # x[0] is year
        
        # Print summary for each year
        print("\nProcessing Summary:")
        print("-" * 40)
        for year, results in temp_results:
            print(f"Year {year}: {len(results):,} records")
            all_results.extend(results)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print("\nPerformance Metrics:")
        print("-" * 40)
        print(f"Total Processing Time: {total_time:.2f} seconds")
        print(f"Average Time per Year: {total_time/total_years:.2f} seconds")
        print(f"Total Records: {len(all_results):,}")
        
        return all_results

# Example usage
if __name__ == "__main__":
    # Configuration
    config = DBConfig(
        username="your_username",
        password="your_password",
        host="your_host",
        port=1521,
        service_name="your_service"
    )
    
    # Sample query - Make sure your query includes YR column
    query = """
        SELECT 
            t.*,
            EXTRACT(YEAR FROM your_date_column) as YR
        FROM your_table t
        WHERE your_conditions = 1
    """
    
    # Initialize query processor
    threaded_query = OracleYearPartitionedQuery(
        config,
        start_year=2006,
        end_year=2023,
        thick_mode=False
    )
    
    # Execute with 4 concurrent threads
    results = threaded_query.execute_parallel(query, max_concurrent_threads=4)
