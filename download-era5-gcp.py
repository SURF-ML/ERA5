import os
from datetime import datetime, timedelta
import logging
from google.cloud import storage
from joblib import Parallel, delayed
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ERA5ZarrDownloader:
    def __init__(self, bucket_name="gcp-public-data-arco-era5", num_clients=32):
        """Initialize the ERA5 zarr downloader with multiple GCP clients."""
        self.bucket_name = bucket_name
        self.num_clients = num_clients
        
        # Create multiple storage clients for parallel access
        self.clients = [storage.Client() for _ in range(num_clients)]
        self.buckets = [client.bucket(bucket_name) for client in self.clients]
        
        # ERA5 zarr variable mappings
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
        
        # Zarr dataset path
        self.zarr_base_path = "ar/1959-2022-wb13-6h-0p25deg-chunk-1.zarr-v2"
        
    def get_bucket_for_job(self, job_index):
        """Get bucket for specific job index (no locking needed)."""
        return self.buckets[job_index % self.num_clients]
    
    def is_file_complete(self, file_path):
        """Check if a file exists and is completely downloaded."""
        if not os.path.exists(file_path):
            return False
        
        try:
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.warning(f"Empty file detected: {file_path}")
                return False
            return True
        except (OSError, IOError):
            logger.warning(f"Error checking file: {file_path}")
            return False
    
    def get_zarr_chunk_paths(self, variables, var_type="surface"):
        """Get all zarr chunk paths for specified variables."""
        var_mapping = self.surface_vars if var_type == "surface" else self.atmos_vars
        logger.info(f"Searching for {var_type} zarr chunks for variables: {variables}")
        
        chunk_paths = []
        bucket = self.buckets[0]

        def list_var_chunks(var_short):
            paths = []
            if var_short in var_mapping:
                var_full = var_mapping[var_short]
                prefix = f"{self.zarr_base_path}/{var_full}"
                try:
                    for blob in bucket.list_blobs(prefix=prefix):
                        if var_full in blob.name:
                            paths.append(blob.name)
                except Exception as e:
                    logger.error(f"Error listing blobs for {var_short}: {str(e)}")
            return paths

        from concurrent.futures import ThreadPoolExecutor, as_completed
        with ThreadPoolExecutor() as executor:
            futures = {executor.submit(list_var_chunks, var): var for var in variables}
            for future in as_completed(futures):
                chunk_paths.extend(future.result())

        logger.info(f"Found {len(chunk_paths)} zarr chunks for {var_type} variables")
        return chunk_paths
    
    def download_single_chunk(self, args):
        """Download a single zarr chunk from GCS."""
        chunk_path, local_dir, job_index = args
        
        try:
            # Create local directory structure
            local_file_path = Path(local_dir) / Path(chunk_path)
            local_file_path.parent.mkdir(parents=True, exist_ok=True)
            local_file_path = str(local_file_path)
            
            # # Skip if file already exists and is complete
            # if self.is_file_complete(local_file_path):
            #     logger.info(f"File already complete: {os.path.basename(chunk_path)}")
            #     return local_file_path
            
            # Get bucket for this specific job (no locking needed)
            bucket = self.get_bucket_for_job(job_index)
            
            # Download the chunk
            blob = bucket.blob(chunk_path)
            blob.download_to_filename(local_file_path)
            
            # logger.info(f"Downloaded: {os.path.basename(chunk_path)} (client {job_index % self.num_clients})")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Error downloading {chunk_path}: {str(e)}")
            return None
    
    def download_parallel(self, chunk_paths, local_dir="era5-zarr", jobs_per_client=1):
        """Download multiple zarr chunks in parallel using joblib."""
        n_jobs = self.num_clients * jobs_per_client
        logger.info(f"Starting parallel download with {n_jobs} jobs ({self.num_clients} clients × {jobs_per_client} jobs/client)...")
        
        # Create job arguments with indices for client selection
        job_args = [(chunk_path, local_dir, i) for i, chunk_path in enumerate(chunk_paths)]
        
        # Use joblib Parallel for downloading
        downloaded_files = Parallel(n_jobs=n_jobs, backend='threading')(
            delayed(self.download_single_chunk)(args) 
            for args in job_args
        )
        
        # Filter out None results (failed downloads)
        successful_downloads = [f for f in downloaded_files if f is not None]
        
        logger.info(f"Successfully downloaded {len(successful_downloads)}/{len(chunk_paths)} files")
        return successful_downloads


def debug_small_download():
    """Debug function to test downloading with 2 clients and minimal data."""
    logger.info("=" * 60)
    logger.info("RUNNING DEBUG TEST - 2 clients, 1 surface + 1 atmospheric variable")
    logger.info("=" * 60)
    
    # Create downloader with only 2 clients for testing
    downloader = ERA5ZarrDownloader(num_clients=2)
    
    
    # Download with 1 job per client (2 clients × 1 job = 2 total jobs)
        # Get chunks for just one surface variable
    surface_chunks = downloader.get_zarr_chunk_paths(
        variables=['msl', '2t'],  # Just 2m temperature
        var_type="surface"
    )
    logger.info(f"Found {len(surface_chunks)} surface chunks")

    surface_files = downloader.download_parallel(
        chunk_paths=surface_chunks,
        local_dir="era5-zarr-debug/surface",
        jobs_per_client=16
    )
    # Limit to first 1000 chunks each for testing
    surface_chunks = surface_chunks[:10000]

    logger.info(f"Debug surface download: {len(surface_files)} files")
    

    # Get chunks for just one atmospheric variable  
    atmos_chunks = downloader.get_zarr_chunk_paths(
        variables=['q', 't'],   # Just temperature
        var_type="atmospheric"
    )

    logger.info(f"Found {len(atmos_chunks)} surface chunks")

    atmos_files = downloader.download_parallel(
        chunk_paths=atmos_chunks,
        local_dir="era5-zarr-debug/atmospheric",
        jobs_per_client=1
    )

    atmos_chunks = atmos_chunks[:10000]
    logger.info(f"Debug atmospheric download: {len(atmos_files)} files")

    logger.info("Debug test completed!")
    return True


def download_surface_zarr(variables=['msl', '10u', '10v', '2t'], jobs_per_client=1):
    """Download surface ERA5 zarr data."""
    downloader = ERA5ZarrDownloader(num_clients=2)
    jobs_per_client=16
    
    logger.info(f"Downloading surface variables: {variables}")
    chunk_paths = downloader.get_zarr_chunk_paths(
        variables=variables,
        var_type="surface"
    )
    
    if not chunk_paths:
        logger.warning("No zarr chunks found for the specified criteria")
        return []
    
    logger.info(f"Found {len(chunk_paths)} surface zarr chunks to download")
    
    return downloader.download_parallel(
        chunk_paths=chunk_paths,
        local_dir="era5-zarr/surface",
        jobs_per_client=jobs_per_client
    )


def download_atmospheric_zarr(variables=['q', 't', 'u', 'v', 'z'], jobs_per_client=1):
    """Download atmospheric ERA5 zarr data (pressure levels)."""
    downloader = ERA5ZarrDownloader(num_clients=4)
    jobs_per_client=8
    
    logger.info(f"Downloading atmospheric variables: {variables}")
    chunk_paths = downloader.get_zarr_chunk_paths(
        variables=variables,
        var_type="atmospheric"
    )
    
    if not chunk_paths:
        logger.warning("No zarr chunks found for the specified criteria")
        return []
    
    logger.info(f"Found {len(chunk_paths)} atmospheric zarr chunks to download")
    
    return downloader.download_parallel(
        chunk_paths=chunk_paths,
        local_dir="era5-zarr/pressure-level", 
        jobs_per_client=jobs_per_client
    )


def main():
    """Download ERA5 zarr data."""
    
    # Download atmospheric data (5 variables)
    logger.info("=" * 60)
    logger.info("DOWNLOADING ATMOSPHERIC ZARR DATA")
    logger.info("=" * 60)
    
    atmospheric_files = download_atmospheric_zarr(
        variables=['q', 't', 'u', 'v', 'z'],
        jobs_per_client=16  # 32 clients × 1 job = 32 total jobs
    )
    
    logger.info(f"Downloaded {len(atmospheric_files)} atmospheric zarr chunks")
    
    # # Download surface data (4 variables)
    # logger.info("=" * 60)
    # logger.info("DOWNLOADING SURFACE ZARR DATA")
    # logger.info("=" * 60)
    
    # surface_files = download_surface_zarr(
    #     variables=['msl', '10u', '10v', '2t'],
    #     jobs_per_client=16  # 32 clients × 1 job = 32 total jobs
    # )
    
    # logger.info(f"Downloaded {len(surface_files)} surface zarr chunks")
    
    # # Summary
    # total_files = len(surface_files) + len(atmospheric_files)
    # logger.info("=" * 60)
    # logger.info(f"DOWNLOAD COMPLETE: {total_files} zarr chunks")
    # logger.info("=" * 60)


if __name__ == "__main__":
    # Requirements:
    # pip install google-cloud-storage joblib
    
    # Authentication:
    # export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account-key.json"
    # or: gcloud auth application-default login
    
    # Uncomment one of these:
    
    # Run debug test (recommended first)
    # debug_small_download()
    
    # Run full download (comment out debug first)
    main()