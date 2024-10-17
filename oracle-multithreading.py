import oracledb
import threading
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

class OracleThreadedQuery:
    def __init__(self, db_config: DBConfig, num_threads: int = 4, thick_mode: bool = False):
        self.db_config = db_config
        self.num_threads = num_threads
        self.result_queue = Queue()
        
        # Initialize thick mode if requested
        if thick_mode:
            oracledb.init_oracle_client()
    
    def get_connection(self):
        return oracledb.connect(
            user=self.db_config.username,
            password=self.db_config.password,
            dsn=self.db_config.get_dsn()
        )
    
    def get_partition_ranges(self, total_rows: int) -> List[tuple]:
        """Calculate the ranges for each thread based on ROWNUM"""
        chunk_size = total_rows // self.num_threads
        ranges = []
        for i in range(self.num_threads):
            start = i * chunk_size + 1
            end = (i + 1) * chunk_size if i < self.num_threads - 1 else total_rows
            ranges.append((start, end))
        return ranges
    
    def process_chunk(self, query: str, row_range: tuple) -> List[Dict]:
        """Process a chunk of the query based on ROWNUM range"""
        start, end = row_range
        wrapped_query = f"""
            SELECT * FROM (
                SELECT a.*, ROWNUM rnum FROM (
                    {query}
                ) a WHERE ROWNUM <= {end}
            ) WHERE rnum >= {start}
        """
        
        results = []
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(wrapped_query)
                columns = [col[0] for col in cursor.description]
                for row in cursor:
                    results.append(dict(zip(columns, row)))
        
        self.result_queue.put(results)
        return results
    
    def execute_parallel(self, query: str) -> List[Dict]:
        """Execute the query in parallel using multiple threads"""
        # First, get total count
        count_query = f"SELECT COUNT(*) FROM ({query})"
        with self.get_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(count_query)
                total_rows = cursor.fetchone()[0]
        
        ranges = self.get_partition_ranges(total_rows)
        
        # Execute queries in parallel using ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = [
                executor.submit(self.process_chunk, query, row_range)
                for row_range in ranges
            ]
        
        # Combine all results
        all_results = []
        while not self.result_queue.empty():
            all_results.extend(self.result_queue.get())
        
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
    
    # Sample query
    query = """
        SELECT *
        FROM large_table
        WHERE some_condition = 1
        ORDER BY id
    """
    
    # Initialize and execute
    # Set thick_mode=True if using Oracle Client libraries
    threaded_query = OracleThreadedQuery(config, num_threads=4, thick_mode=False)
    
    start_time = time.time()
    results = threaded_query.execute_parallel(query)
    end_time = time.time()
    
    print(f"Processed {len(results)} rows in {end_time - start_time:.2f} seconds")
