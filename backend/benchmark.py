#!/usr/bin/env python3
# benchmark.py - Performance benchmarking tool for dynamoDB implementation

import argparse
import json
import random
import requests
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
import numpy as np
from tqdm import tqdm

# Constants
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5000
DEFAULT_WORKERS = 4
DEFAULT_OPS = 1000

def generate_random_key(length=10):
    """Generate a random string key"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def generate_random_value(length=100):
    """Generate a random string value"""
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=length))

def perform_put(base_url, key, value):
    """Perform a PUT operation and measure latency"""
    data = {"value": value}
    start_time = time.time()
    try:
        response = requests.put(
            f"{base_url}/kv/{key}",
            json=data,
            headers={"Content-Type": "application/json"},
            timeout=5
        )
        latency = time.time() - start_time
        return {
            "success": response.status_code in (200, 201),
            "latency": latency,
            "status_code": response.status_code
        }
    except Exception as e:
        latency = time.time() - start_time
        return {
            "success": False,
            "latency": latency,
            "error": str(e)
        }

def perform_get(base_url, key):
    """Perform a GET operation and measure latency"""
    start_time = time.time()
    try:
        response = requests.get(
            f"{base_url}/kv/{key}",
            timeout=5
        )
        latency = time.time() - start_time
        return {
            "success": response.status_code == 200,
            "latency": latency,
            "status_code": response.status_code
        }
    except Exception as e:
        latency = time.time() - start_time
        return {
            "success": False,
            "latency": latency,
            "error": str(e)
        }

def run_write_benchmark(base_url, num_operations, num_workers):
    """Run a write benchmark with the specified number of operations and workers"""
    print(f"Running write benchmark with {num_operations} operations using {num_workers} workers")
    
    # Generate keys and values in advance
    keys = [generate_random_key() for _ in range(num_operations)]
    values = [generate_random_value() for _ in range(num_operations)]
    
    results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Use tqdm to display a progress bar
        futures = []
        for i in range(num_operations):
            future = executor.submit(perform_put, base_url, keys[i], values[i])
            futures.append(future)
        
        for future in tqdm(futures, total=num_operations, desc="PUT Operations"):
            results.append(future.result())
    
    return results

def run_read_benchmark(base_url, keys, num_workers):
    """Run a read benchmark for the specified keys using the specified number of workers"""
    print(f"Running read benchmark for {len(keys)} keys using {num_workers} workers")
    
    results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Use tqdm to display a progress bar
        futures = []
        for key in keys:
            future = executor.submit(perform_get, base_url, key)
            futures.append(future)
        
        for future in tqdm(futures, total=len(keys), desc="GET Operations"):
            results.append(future.result())
    
    return results

def run_mixed_benchmark(base_url, num_operations, read_percentage, num_workers):
    """Run a mixed read/write benchmark with the specified operations, read percentage and workers"""
    print(f"Running mixed benchmark with {num_operations} operations " + 
          f"({read_percentage}% reads) using {num_workers} workers")
    
    # Determine how many operations are reads vs writes
    num_reads = int(num_operations * (read_percentage / 100))
    num_writes = num_operations - num_reads
    
    # First, perform writes to establish data
    print("Phase 1: Establishing data with write operations")
    keys = [generate_random_key() for _ in range(num_writes)]
    values = [generate_random_value() for _ in range(num_writes)]
    
    write_results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for i in range(num_writes):
            future = executor.submit(perform_put, base_url, keys[i], values[i])
            futures.append(future)
        
        for future in tqdm(futures, total=num_writes, desc="Initial PUT Operations"):
            write_results.append(future.result())
    
    # Now perform the mixed workload (all reads in this case, since we did writes already)
    print("Phase 2: Performing read operations")
    # Randomly select keys for reads (with replacement to simulate real-world access patterns)
    read_keys = random.choices(keys, k=num_reads)
    
    read_results = []
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        for key in read_keys:
            future = executor.submit(perform_get, base_url, key)
            futures.append(future)
        
        for future in tqdm(futures, total=num_reads, desc="GET Operations"):
            read_results.append(future.result())
    
    # Combine results
    return {
        "writes": write_results,
        "reads": read_results
    }

def analyze_results(results, benchmark_type):
    """Analyze benchmark results and print summary statistics"""
    if benchmark_type == "mixed":
        write_results = results["writes"]
        read_results = results["reads"]
        
        print("\n===== WRITE OPERATIONS =====")
        analyze_single_results(write_results)
        
        print("\n===== READ OPERATIONS =====")
        analyze_single_results(read_results)
        
        print("\n===== COMBINED STATISTICS =====")
        all_results = write_results + read_results
        analyze_single_results(all_results)
        
        return
    
    analyze_single_results(results)

def analyze_single_results(results):
    """Analyze a single set of benchmark results"""
    # Extract latencies
    latencies = [r["latency"] for r in results]
    
    # Calculate success rate
    success_count = sum(1 for r in results if r["success"])
    success_rate = (success_count / len(results)) * 100
    
    # Calculate statistics
    if latencies:
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        
        # Calculate percentiles
        p50 = np.percentile(latencies, 50)
        p90 = np.percentile(latencies, 90)
        p95 = np.percentile(latencies, 95)
        p99 = np.percentile(latencies, 99)
        
        # Print summary
        print(f"Total operations: {len(results)}")
        print(f"Success rate: {success_rate:.2f}%")
        print(f"Average latency: {avg_latency*1000:.2f} ms")
        print(f"Min latency: {min_latency*1000:.2f} ms")
        print(f"Max latency: {max_latency*1000:.2f} ms")
        print(f"50th percentile (median): {p50*1000:.2f} ms")
        print(f"90th percentile: {p90*1000:.2f} ms")
        print(f"95th percentile: {p95*1000:.2f} ms")
        print(f"99th percentile: {p99*1000:.2f} ms")
        
        # Calculate throughput
        total_time = sum(latencies)
        if total_time > 0:
            ops_per_second = len(results) / total_time
            print(f"Estimated throughput: {ops_per_second:.2f} operations/second")
    else:
        print("No latency data available.")

def plot_latency_distribution(results, benchmark_type, output_file=None):
    """Plot latency distribution as a histogram"""
    plt.figure(figsize=(10, 6))
    
    if benchmark_type == "mixed":
        write_latencies = [r["latency"] * 1000 for r in results["writes"]]
        read_latencies = [r["latency"] * 1000 for r in results["reads"]]
        
        plt.hist(write_latencies, alpha=0.5, label="Write Latencies", bins=20)
        plt.hist(read_latencies, alpha=0.5, label="Read Latencies", bins=20)
        plt.legend()
    else:
        latencies = [r["latency"] * 1000 for r in results]
        plt.hist(latencies, bins=20)
    
    plt.title("Operation Latency Distribution")
    plt.xlabel("Latency (ms)")
    plt.ylabel("Count")
    plt.grid(True, linestyle='--', alpha=0.7)
    
    if output_file:
        plt.savefig(output_file)
        print(f"Latency distribution plot saved to {output_file}")
    else:
        plt.show()

def plot_latency_over_time(results, benchmark_type, output_file=None):
    """Plot latency over time as a line chart"""
    plt.figure(figsize=(12, 6))
    
    if benchmark_type == "mixed":
        write_latencies = [r["latency"] * 1000 for r in results["writes"]]
        read_latencies = [r["latency"] * 1000 for r in results["reads"]]
        
        plt.plot(range(len(write_latencies)), write_latencies, 'b-', alpha=0.5, label="Write Latencies")
        plt.plot(range(len(read_latencies)), read_latencies, 'r-', alpha=0.5, label="Read Latencies")
        plt.legend()
    else:
        latencies = [r["latency"] * 1000 for r in results]
        plt.plot(range(len(latencies)), latencies, 'g-', alpha=0.7)
    
    plt.title("Latency Over Operation Sequence")
    plt.xlabel("Operation Number")
    plt.ylabel("Latency (ms)")
    plt.grid(True, linestyle='--', alpha=0.7)
    
    if output_file:
        plt.savefig(output_file)
        print(f"Latency over time plot saved to {output_file}")
    else:
        plt.show()

def check_cluster_health(base_url):
    """Check if the cluster is healthy before running benchmarks"""
    try:
        response = requests.get(f"{base_url}/admin/cluster", timeout=2)
        if response.status_code == 200:
            try:
                cluster_info = response.json()
                print("Cluster status:")
                if "summary" in cluster_info:
                    summary = cluster_info["summary"]
                    print(f"  Total nodes: {summary.get('total', 'N/A')}")
                    print(f"  Alive nodes: {summary.get('alive', 'N/A')}")
                return True
            except json.JSONDecodeError:
                print("Warning: Could not parse cluster status")
                return True
        else:
            print(f"Warning: Cluster health check failed with status code {response.status_code}")
            return False
    except Exception as e:
        print(f"Warning: Cluster health check failed: {str(e)}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Benchmark tool for DynamoDB implementation")
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Host (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Port (default: {DEFAULT_PORT})")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, 
                        help=f"Number of concurrent workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--operations", type=int, default=DEFAULT_OPS, 
                        help=f"Number of operations to perform (default: {DEFAULT_OPS})")
    parser.add_argument("--type", choices=["write", "read", "mixed"], default="mixed", 
                        help="Benchmark type: write, read, or mixed (default: mixed)")
    parser.add_argument("--read-pct", type=int, default=80, 
                        help="Percentage of read operations in mixed benchmark (default: 80)")
    parser.add_argument("--plot", action="store_true", help="Generate plots")
    parser.add_argument("--output", help="Output file prefix for plots")
    
    args = parser.parse_args()
    
    base_url = f"http://{args.host}:{args.port}"
    
    # Check cluster health before starting
    if not check_cluster_health(base_url):
        print("Warning: Cluster may not be healthy. Continue anyway? (y/n)")
        response = input().strip().lower()
        if response != 'y' and response != 'yes':
            print("Benchmark aborted.")
            sys.exit(1)
    
    start_time = time.time()
    
    # Run the requested benchmark
    if args.type == "write":
        results = run_write_benchmark(base_url, args.operations, args.workers)
        benchmark_type = "write"
    elif args.type == "read":
        # For read benchmarks, we need to first write data to read
        print("First writing data that will be read during benchmark...")
        keys = [generate_random_key() for _ in range(args.operations)]
        values = [generate_random_value() for _ in range(args.operations)]
        
        for i in tqdm(range(args.operations), desc="Preparing data"):
            perform_put(base_url, keys[i], values[i])
        
        # Now run the read benchmark
        results = run_read_benchmark(base_url, keys, args.workers)
        benchmark_type = "read"
    else:  # mixed
        results = run_mixed_benchmark(base_url, args.operations, args.read_pct, args.workers)
        benchmark_type = "mixed"
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\nTotal benchmark time: {total_time:.2f} seconds")
    
    # Analyze results
    print("\n===== BENCHMARK RESULTS =====")
    analyze_results(results, benchmark_type)
    
    # Generate plots if requested
    if args.plot:
        if args.output:
            latency_dist_file = f"{args.output}_latency_dist.png"
            latency_time_file = f"{args.output}_latency_time.png"
        else:
            latency_dist_file = None
            latency_time_file = None
        
        plot_latency_distribution(results, benchmark_type, latency_dist_file)
        plot_latency_over_time(results, benchmark_type, latency_time_file)

if __name__ == "__main__":
    main()