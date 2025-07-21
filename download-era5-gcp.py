import os
from datetime import datetime, timedelta
import logging
from google.cloud import storage
from joblib import Parallel, delayed
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import timedelta

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ERA5Downloader:
    def __init__(self, bucket_name="gcp-public-data-arco-era5"):
        """Initialize the ERA5 downloader with GCP bucket."""
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        
        # ERA5 variable mappings
        self.surface_vars = {
            'msl': 'mean_sea_level_pressure',
            '10u': '10m_u_component_of_wind', 
            '10v': '10m_v_component_of_wind',
            '2t': '2m_temperature'
        }
        
        self.atmos_vars = {
            'q': 'specific_humidity',
            't': 'temperature',
            'u': 'u_component_of_wind',
            'v': 'v_component_of_wind', 
            'z': 'geopotential'
        }
        
        self.pressure_levels = {50, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000}
    
    def is_file_complete(self, file_path):
        """Check if a file exists and is completely downloaded."""
        if not os.path.exists(file_path):
            return False
        
        # Check if file size is reasonable (> 0 bytes)
        try:
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.warning(f"Empty file detected: {file_path}")
                return False
            
            # Optional: Try to open file to verify it's not corrupted
            # This is a basic check - you might want to add more validation
            return True
            
        except (OSError, IOError):
            logger.warning(f"Error checking file: {file_path}")
            return False
    
    def generate_file_paths(self, variables, start_date, end_date, level_type="single-level"):
        """Generate GCS file paths for ERA5 data with threading."""
        var_mapping = self.surface_vars if level_type == "single-level" else self.atmos_vars

        def process_day(current_date):
            year = current_date.year
            month = current_date.month
            day = current_date.day
            daily_paths = []
            print(year, month, day)

            for var_short in variables:
                if var_short not in var_mapping:
                    logger.warning(f"Variable {var_short} not found in {level_type} variables")
                    continue

                var_full = var_mapping[var_short]
                base_path = (
                    f"raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/"
                    if level_type == "single-level"
                    else f"raw/date-variable-pressure_level/{year:04d}/{month:02d}/{day:02d}/"
                )

                try:
                    blobs = list(self.bucket.list_blobs(prefix=base_path))
                    for blob in blobs:
                        if (
                            var_full in blob.name
                            and blob.name.endswith('.nc')
                            and (level_type != 'pressure-level' or int(Path(blob.name).stem) in self.pressure_levels)
                        ):
                            daily_paths.append(blob.name)
                except Exception as e:
                    logger.error(f"Error listing files for {base_path}: {str(e)}")

            return daily_paths

        file_paths = []
        with ThreadPoolExecutor(max_workers=32) as executor:
            futures = []
            current_date = start_date
            while current_date <= end_date:
                futures.append(executor.submit(process_day, current_date))
                current_date += timedelta(days=1)

            for f in as_completed(futures):
                file_paths.extend(f.result())

        return file_paths    
    
    def generate_file_paths2(self, variables, start_date, end_date, level_type="single-level"):
        """Generate GCS file paths for ERA5 data."""
        file_paths = []
        current_date = start_date
        
        # Map short names to full names
        var_mapping = self.surface_vars if level_type == "single-level" else self.atmos_vars
        
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day

            print(f"Generating files for level_type: {level_type}, {year}/{month}")
            
            for var_short in variables:
                if var_short not in var_mapping:
                    logger.warning(f"Variable {var_short} not found in {level_type} variables")
                    continue
                
                var_full = var_mapping[var_short]
                
                # ERA5 ARCO raw folder structure
                if level_type == "single-level":
                    base_path = f"raw/date-variable-single_level/{year:04d}/{month:02d}/{day:02d}/"
                else:
                    base_path = f"raw/date-variable-pressure_level/{year:04d}/{month:02d}/{day:02d}/"
               
                # List files in this directory
                try:
                    blobs = list(self.bucket.list_blobs(prefix=base_path))
                    
                    for blob in blobs:
                        # Check if file contains the variable and is NetCDF (skip zarr)
                        
                        if (var_full in blob.name and blob.name.endswith('.nc') and (level_type != 'pressure-level' or int(Path(blob.name).stem) in self.pressure_levels)): 
                            file_paths.append(base_path + blob.name)
                            
                except Exception as e:
                    logger.error(f"Error listing files for {base_path}: {str(e)}")
            
            current_date += timedelta(days=1)
        
        return file_paths
    
    def download_single_file(self, file_path, local_dir="era5-gcp"):
        """Download a single file from GCS."""
        try:
            # Create local directory structure
            local_file_path = Path(local_dir) / Path(file_path)
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            local_file_path = str(local_file_path)
            
            # Skip if file already exists and is complete
            if self.is_file_complete(local_file_path):
                logger.info(f"File already complete: {local_file_path}")
                return local_file_path
            
            
            # Download the file
            blob = self.bucket.blob(file_path)
            blob.download_to_filename(local_file_path)
            
            logger.info(f"Downloaded: {os.path.basename(file_path)}")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Error downloading {file_path}: {str(e)}")
            return None
    
    def download_parallel(self, file_paths, local_dir="era5-gcp", n_jobs=32):
        """Download multiple files in parallel using joblib."""
        logger.info(f"Starting parallel download with {n_jobs} jobs...")
        
        # Use joblib Parallel for downloading
        downloaded_files = Parallel(n_jobs=n_jobs, backend='threading')(
            delayed(self.download_single_file)(file_path, local_dir) 
            for file_path in file_paths
        )
        
        # Filter out None results (failed downloads)
        successful_downloads = [f for f in downloaded_files if f is not None]
        
        logger.info(f"Successfully downloaded {len(successful_downloads)}/{len(file_paths)} files")
        return successful_downloads


def download_surface_data(start_date, end_date, variables=['msl', '10u', '10v', '2t'], n_jobs=32):
    """Download surface ERA5 data."""
    downloader = ERA5Downloader()
    
    logger.info(f"Downloading surface variables: {variables}")
    file_paths = downloader.generate_file_paths(
        variables=variables,
        start_date=start_date,
        end_date=end_date,
        level_type="single-level"
    )
    
    if not file_paths:
        logger.warning("No files found for the specified criteria")
        return []
    
    logger.info(f"Found {len(file_paths)} surface files to download")
    
    return downloader.download_parallel(
        file_paths=file_paths,
        local_dir="era5-gcp/surface",
        n_jobs=n_jobs
    )


def download_atmospheric_data(start_date, end_date, variables=['q', 't', 'u', 'v', 'z'], n_jobs=32):
    """Download atmospheric ERA5 data (pressure levels)."""
    downloader = ERA5Downloader()
    
    logger.info(f"Downloading atmospheric variables: {variables}")
    file_paths = downloader.generate_file_paths(
        variables=variables,
        start_date=start_date,
        end_date=end_date,
        level_type="pressure-level"
    )
    
    if not file_paths:
        logger.warning("No files found for the specified criteria")
        return []
    
    logger.info(f"Found {len(file_paths)} atmospheric files to download")
    
    return downloader.download_parallel(
        file_paths=file_paths,
        local_dir="era5-gcp/pressure-level", 
        n_jobs=n_jobs
    )


def main():
    """Example usage of the ERA5 downloader."""
    
    # Date range from 01/01/1959 to 30/06/2025 (as specified)
    # Start with a smaller range for testing
    start_date = datetime(1959, 1, 1)
    
    end_date = datetime(2025, 7, 2)  # Start small, then expand
    #end_date = datetime(1959, 2, 1)  # Start small, then expand
    # Download atmospheric data (5 variables, 13 pressure levels)
    logger.info("=" * 60)
    logger.info("DOWNLOADING ATMOSPHERIC DATA")
    logger.info("=" * 60)
    
    atmospheric_files = download_atmospheric_data(
        start_date=start_date,
        end_date=end_date,
        variables=['q', 't', 'u', 'v', 'z'],  # All 5 atmospheric variables
        n_jobs=32
    )
    
    logger.info(f"Downloaded {len(atmospheric_files)} atmospheric files")
    # Download surface data (4 variables)
    logger.info("=" * 60)
    logger.info("DOWNLOADING SURFACE DATA")
    logger.info("=" * 60)
    
    surface_files = download_surface_data(
        start_date=start_date,
        end_date=end_date,
        variables=['msl', '10u', '10v', '2t'],  # All 4 surface variables
        n_jobs=32
    )
    
    logger.info(f"Downloaded {len(surface_files)} surface files")
    
    
    # Summary
    total_files = len(surface_files) + len(atmospheric_files)
    logger.info("=" * 60)
    logger.info(f"DOWNLOAD COMPLETE: {total_files} files")
    logger.info("=" * 60)


if __name__ == "__main__":
    # Requirements:
    # pip install google-cloud-storage joblib
    
    # Note: This script only downloads NetCDF files, completely skipping zarr files
    # Files are saved to era5-gcp/ directory with surface/ and pressure-level/ subdirectories
    
    # Authentication (choose one):
    # 1. export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
    # 2. gcloud auth application-default login
    
    main()
