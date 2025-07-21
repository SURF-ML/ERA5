import cdsapi
import joblib
from datetime import datetime
import calendar
import os
import json
import hashlib
import time
import netCDF4
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('era5_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ERA5Downloader:
    """Stateful ERA5 downloader with resume capability"""
    
    def __init__(self, base_dir="ERA5", state_file="download_state.json"):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(exist_ok=True)
        self.state_file = self.base_dir / state_file
        self.state = self.load_state()
        
        # WeatherBench dataset configuration
        self.datasets = {
            'surface': {
                'product': 'reanalysis-era5-single-levels',
                'variables': [
                    'mean_sea_level_pressure',      # msl
                    '10m_u_component_of_wind',      # 10u
                    '10m_v_component_of_wind',      # 10v
                    '2m_temperature',               # 2t
                ],
                'levels': None
            },
            'atmospheric': {
                'product': 'reanalysis-era5-pressure-levels',
                'variables': [
                    'specific_humidity',            # q
                    'temperature',                  # t
                    'u_component_of_wind',          # u
                    'v_component_of_wind',          # v
                    'geopotential',                 # z
                ],
                'pressure_levels': ['50', '100', '150', '200', '250', '300', 
                          '400', '500', '600', '700', '850', '925', '1000']
            }
        }
        
        # Common parameters
        self.times = ['00:00', '06:00', '12:00', '18:00']
        self.grid = [0.25, 0.25]
        
        # Date range: 1959-01-01 to 2025-06-30
        self.start_date = datetime(1959, 1, 1)
        self.end_date = datetime(2025, 6, 30)
        
        # Status definitions
        self.STATUS_PENDING = "pending"
        self.STATUS_DOWNLOADING = "downloading"
        self.STATUS_COMPLETED = "completed"
        self.STATUS_FAILED = "failed"
    
    def load_state(self):
        """Load download state from file"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading state: {e}")
                return {}
        return {}
    
    def save_state(self):
        """Save current state to file"""
        try:
            with open(self.state_file, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    def generate_tasks(self):
        """Generate all download tasks"""
        tasks = []
        current = self.start_date
        
        while current <= self.end_date:
            year = current.year
            month = current.month
            
            # Get days in month, but don't exceed end_date
            last_day = calendar.monthrange(year, month)[1]
            month_end = min(datetime(year, month, last_day), self.end_date)
            days = [f"{day:02d}" for day in range(1, month_end.day + 1)]
            
            # Create tasks for each dataset type
            for dataset_name in self.datasets.keys():
                task = {
                    'id': f"{dataset_name}_{year}_{month:02d}",
                    'dataset': dataset_name,
                    'year': year,
                    'month': month,
                    'days': days,
                    'filename': self.base_dir / f"era5_{dataset_name}_{year}_{month:02d}.nc"
                }
                tasks.append(task)
            
            # Move to next month
            if month == 12:
                current = datetime(year + 1, 1, 1)
            else:
                current = datetime(year, month + 1, 1)
        
        return tasks
    
    def verify_file(self, filepath):
        """Verify NetCDF file integrity"""
        try:
            if not filepath.exists() or filepath.stat().st_size < 1000:
                return False
            
            with netCDF4.Dataset(filepath, 'r') as nc:
                # Check basic structure
                required_dims = ['time']
                lat_dims = ['latitude', 'lat']
                lon_dims = ['longitude', 'lon']
                
                if not any(dim in nc.dimensions for dim in required_dims):
                    return False
                if not any(dim in nc.dimensions for dim in lat_dims):
                    return False
                if not any(dim in nc.dimensions for dim in lon_dims):
                    return False
                if len(nc.dimensions['time']) == 0:
                    return False
            
            return True
        except Exception as e:
            logger.warning(f"File verification failed for {filepath}: {e}")
            return False
    
    def update_task_status(self, task_id, status, **kwargs):
        """Update task status"""
        if task_id not in self.state:
            self.state[task_id] = {}
        
        self.state[task_id]['status'] = status
        self.state[task_id]['timestamp'] = datetime.now().isoformat()
        
        for key, value in kwargs.items():
            self.state[task_id][key] = value
        
        self.save_state()
    
    def download_task(self, task, max_retries=3):
        """Download a single task"""
        task_id = task['id']
        filename = task['filename']
        
        # Check if already completed
        if (task_id in self.state and 
            self.state[task_id]['status'] == self.STATUS_COMPLETED and
            self.verify_file(filename)):
            logger.info(f"Skipping {task_id} - already completed")
            return True
        
        # Mark as downloading
        self.update_task_status(task_id, self.STATUS_DOWNLOADING)
        
        # Get dataset configuration
        dataset_config = self.datasets[task['dataset']]
        
        # Build request parameters
        request_params = {
            'product_type': 'reanalysis',
            'variable': dataset_config['variables'],
            'year': str(task['year']),
            'month': f"{task['month']:02d}",
            'day': task['days'],
            'time': self.times,
            'data_format': 'netcdf',
            'download_format': 'unarchived',
        }
        
        # Add pressure levels if needed
        #if dataset_config['levels'] is not None:
        #    request_params['pressure_level'] = dataset_config['levels']
        
        # Download with retries
                
        c = cdsapi.Client()
        c.retrieve(dataset_config['product'], request_params, str(filename))
    
    def calculate_checksum(self, filepath):
        """Calculate MD5 checksum"""
        if not filepath.exists():
            return None
        
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            logger.error(f"Error calculating checksum: {e}")
            return None
    
    def get_pending_tasks(self):
        """Get tasks that need to be downloaded"""
        all_tasks = self.generate_tasks()
        pending_tasks = []
        
        for task in all_tasks:
            task_id = task['id']
            
            if task_id not in self.state:
                pending_tasks.append(task)
            elif self.state[task_id]['status'] in [self.STATUS_PENDING, self.STATUS_FAILED]:
                pending_tasks.append(task)
            elif self.state[task_id]['status'] == self.STATUS_COMPLETED:
                # Double-check file exists and is valid
                if not self.verify_file(task['filename']):
                    logger.warning(f"Completed file {task['filename']} is invalid, re-queueing")
                    pending_tasks.append(task)
        
        return pending_tasks
    
    def download_parallel(self, n_jobs=4):
        """Download all pending tasks in parallel"""
        pending_tasks = self.get_pending_tasks()
        
        if not pending_tasks:
            logger.info("No pending tasks found!")
            return
        
        logger.info(f"Starting download with {n_jobs} parallel jobs")
        logger.info(f"Found {len(pending_tasks)} pending tasks")
        
        # Use joblib for parallel processing
        results = joblib.Parallel(n_jobs=n_jobs, verbose=1)(
            joblib.delayed(self.download_task)(task) for task in pending_tasks
        )
        
        successful = sum(results)
        failed = len(results) - successful
        
        logger.info(f"Download completed: {successful} successful, {failed} failed")
        self.print_status()
    
    def print_status(self):
        """Print current download status"""
        if not self.state:
            print("No downloads started")
            return
        
        status_counts = {}
        for task_info in self.state.values():
            status = task_info.get('status', self.STATUS_PENDING)
            status_counts[status] = status_counts.get(status, 0) + 1
        
        total = sum(status_counts.values())
        all_tasks = len(self.generate_tasks())
        
        print(f"\nDownload Status Report")
        print(f"{'='*40}")
        print(f"Total expected tasks: {all_tasks}")
        print(f"Tasks in state file: {total}")
        print(f"Not started: {all_tasks - total}")
        
        for status, count in status_counts.items():
            percentage = (count / all_tasks * 100) if all_tasks > 0 else 0
            print(f"{status.capitalize()}: {count} ({percentage:.1f}%)")
        
        print(f"{'='*40}")
    
    def cleanup_failed(self):
        """Reset failed tasks and remove invalid files"""
        cleaned = 0
        for task_id, task_info in self.state.items():
            if task_info.get('status') == self.STATUS_FAILED:
                # Reset to pending
                self.update_task_status(task_id, self.STATUS_PENDING)
                cleaned += 1
        
        logger.info(f"Reset {cleaned} failed tasks to pending")

def main():
    """Main function for SLURM execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='ERA5 WeatherBench Downloader')
    parser.add_argument('--jobs', '-j', type=int, default=32, 
                       help='Number of parallel jobs (default: 4)')
    parser.add_argument('--status', action='store_true',
                       help='Show status and exit')
    parser.add_argument('--cleanup', action='store_true',
                       help='Reset failed tasks and exit')
    parser.add_argument('--base-dir', default='ERA5',
                       help='Base directory for downloads (default: ERA5)')
    
    args = parser.parse_args()
    
    # Initialize downloader
    downloader = ERA5Downloader(base_dir=args.base_dir)
    
    if args.status:
        downloader.print_status()
        return
    
    if args.cleanup:
        downloader.cleanup_failed()
        return
    
    # Start download
    logger.info("Starting ERA5 WeatherBench dataset download")
    logger.info(f"Date range: {downloader.start_date.date()} to {downloader.end_date.date()}")
    logger.info(f"Parallel jobs: {args.jobs}")
    
    downloader.download_parallel(n_jobs=args.jobs)

if __name__ == "__main__":
    main()
