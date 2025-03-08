import os
import yaml
import time
import requests
from plexapi.server import PlexServer
from pathlib import Path
from typing import Dict, Any
import logging
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
import backoff
import shutil
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

class PlexArtworkDownloader:
    """
    A tool for downloading artwork from a Plex server.
    This is a read-only tool that only downloads images and does not modify the Plex server in any way.
    All operations are safe and won't affect your media library or server configuration.
    """
    def __init__(self, config_path: str = "config.yml"):
        self.config = self._load_config(config_path)
        self.setup_logging()
        self.setup_http_session()
        self.plex = self._connect_to_plex()
        
        # Track statistics
        self.stats = {
            'total_processed': 0,
            'successful_downloads': 0,
            'failed_downloads': 0,
            'verification_failures': 0,
            'retries': 0
        }
        
        # Track failed items
        self.failed_items = []
        
        # Get the directory where the script is located
        script_dir = Path(__file__).parent.absolute()
        # Create output directory relative to script location
        self.base_output_dir = script_dir / self.config['output_directory']
        self.base_output_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up error log file
        self.error_log_path = script_dir / 'failed_downloads.log'
        
    def log_failure(self, media_type: str, title: str, year: str, error: str, url: str = None):
        """Log a failed download with details."""
        failure_info = {
            'media_type': media_type,
            'title': title,
            'year': year,
            'error': str(error),
            'url': url,
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        self.failed_items.append(failure_info)
        
    def write_error_log(self):
        """Write all failed downloads to a log file."""
        try:
            with open(self.error_log_path, 'w', encoding='utf-8') as f:
                f.write("Failed Downloads Report\n")
                f.write("=====================\n\n")
                f.write(f"Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                
                if not self.failed_items:
                    f.write("No failures reported!\n")
                    return
                
                # Group failures by media type
                movies = [item for item in self.failed_items if item['media_type'] == 'movie']
                shows = [item for item in self.failed_items if item['media_type'] == 'show']
                
                if movies:
                    f.write("\nFailed Movies:\n")
                    f.write("-------------\n")
                    for item in movies:
                        f.write(f"\nTitle: {item['title']} ({item['year']})\n")
                        f.write(f"Error: {item['error']}\n")
                        if item['url']:
                            f.write(f"URL: {item['url']}\n")
                        f.write(f"Time: {item['timestamp']}\n")
                
                if shows:
                    f.write("\nFailed TV Shows:\n")
                    f.write("---------------\n")
                    for item in shows:
                        f.write(f"\nTitle: {item['title']} ({item['year']})\n")
                        f.write(f"Error: {item['error']}\n")
                        if item['url']:
                            f.write(f"URL: {item['url']}\n")
                        f.write(f"Time: {item['timestamp']}\n")
                
                f.write(f"\nTotal Failures: {len(self.failed_items)}\n")
                f.write(f"Movies: {len(movies)}\n")
                f.write(f"TV Shows: {len(shows)}\n")
        except Exception as e:
            self.logger.error(f"Failed to write error log: {e}")

    def setup_http_session(self):
        """Configure requests session with retry logic"""
        retry_strategy = Retry(
            total=5,  # increased number of retries
            backoff_factor=2,  # increased backoff time
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]  # explicitly allow GET
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=2,  # further reduced concurrent connections
            pool_maxsize=2
        )
        self.session = requests.Session()
        # Set higher timeouts for the entire session
        self.session.timeout = (30, 300)  # (connect timeout, read timeout)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _validate_config(self, config: Dict[Any, Any]) -> None:
        """Validate the configuration file structure."""
        required_keys = ['plex_url', 'plex_token', 'output_directory', 'download_threads', 'libraries', 'logging']
        for key in required_keys:
            if key not in config:
                print(f"Error: Missing required configuration key: '{key}'")
                exit(1)
                
        if 'movies' not in config['libraries'] or 'tv_shows' not in config['libraries']:
            print("Error: Configuration must include both 'movies' and 'tv_shows' under 'libraries'")
            exit(1)
            
        if config['plex_token'] == 'YOUR_PLEX_TOKEN':
            print("Error: Please replace 'YOUR_PLEX_TOKEN' with your actual Plex token in config.yml")
            exit(1)

    def _load_config(self, config_path: str) -> Dict[Any, Any]:
        """Load configuration from YAML file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                self._validate_config(config)
                return config
        except FileNotFoundError:
            print(f"Error: Config file '{config_path}' not found.")
            print("Please create a config.yml file with your Plex server details.")
            exit(1)
        except yaml.YAMLError as e:
            print(f"Error: Invalid YAML in config file: {e}")
            exit(1)

    def setup_logging(self):
        """Configure logging based on config settings."""
        logging.basicConfig(
            level=getattr(logging, self.config['logging']['level']),
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.config['logging']['file']),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _connect_to_plex(self) -> PlexServer:
        """Establish connection to Plex server."""
        try:
            return PlexServer(self.config['plex_url'], self.config['plex_token'])
        except Exception as e:
            self.logger.error(f"Failed to connect to Plex server: {e}")
            raise

    def _sanitize_filename(self, filename: str) -> str:
        """Remove invalid characters from filename."""
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '')
        return filename.strip()

    def _verify_image(self, file_path: Path) -> bool:
        """
        Verify that the downloaded image is valid and not corrupt.
        Returns True if image is valid, False otherwise.
        """
        try:
            from PIL import Image
            with Image.open(file_path) as img:
                # Verify the image by attempting to load it
                img.verify()
                # Additional check by loading the image data
                img = Image.open(file_path)
                img.load()
                return True
        except Exception as e:
            self.logger.error(f"Image verification failed for {file_path}: {e}")
            if file_path.exists():
                file_path.unlink()  # Delete corrupt file
            return False

    def _download_image(self, url: str, output_path: Path) -> bool:
        """
        Download image from URL to specified path and verify its integrity.
        This is a read-only operation that only downloads the image without modifying the Plex server.
        Ensures highest quality version of the image is downloaded.
        """
        try:
            # Add the X-Plex-Token to the URL
            if '?' in url:
                url += f'&X-Plex-Token={self.config["plex_token"]}'
            else:
                url += f'?X-Plex-Token={self.config["plex_token"]}'

            response = requests.get(url, timeout=30)  # Added timeout
            response.raise_for_status()
            
            # Create parent directories if they don't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # First write to a temporary file
            temp_path = output_path.with_suffix('.tmp')
            with open(temp_path, 'wb') as f:
                f.write(response.content)
            
            # Verify the downloaded image
            if self._verify_image(temp_path):
                # If verification passes, move to final location
                temp_path.replace(output_path)
                self.logger.debug(f"Successfully downloaded and verified: {output_path}")
                self.stats['successful_downloads'] += 1
                return True
            else:
                self.logger.error(f"Downloaded file failed verification: {output_path}")
                self.stats['verification_failures'] += 1
                if temp_path.exists():
                    temp_path.unlink()  # Clean up temp file
                return False
                
        except requests.RequestException as e:
            self.logger.error(f"Failed to download {url}: {e}")
            self.stats['failed_downloads'] += 1
            return False
        except Exception as e:
            self.logger.error(f"Error processing {url} to {output_path}: {e}")
            self.stats['failed_downloads'] += 1
            return False
        finally:
            # Clean up temp file if it still exists
            if 'temp_path' in locals() and temp_path.exists():
                temp_path.unlink()

    def process_movie(self, movie) -> None:
        """Process artwork for a single movie."""
        try:
            self.stats['total_processed'] += 1
            year = str(movie.year) if movie.year else "Unknown Year"
            movie_dir = self.base_output_dir / 'Movies' / self._sanitize_filename(f"{movie.title} ({year})")
            
            # Add small delay between files to prevent overwhelming the server
            time.sleep(0.1)
            
            # Download full quality poster
            if movie.posterUrl:
                # Remove any quality parameters to get original quality
                poster_url = movie.posterUrl.split('?')[0]
                if not self._download_image(poster_url, movie_dir / "poster.jpg"):
                    self.log_failure('movie', movie.title, year, "Failed to download poster", poster_url)
            
            # Add small delay between files
            time.sleep(0.1)
            
            # Download full quality background
            if movie.artUrl:
                # Remove any quality parameters to get original quality
                art_url = movie.artUrl.split('?')[0]
                if not self._download_image(art_url, movie_dir / "background.jpg"):
                    self.log_failure('movie', movie.title, year, "Failed to download background", art_url)
                
        except Exception as e:
            self.logger.error(f"Error processing movie {movie.title}: {e}")
            self.log_failure('movie', movie.title, year, str(e))

    def process_show(self, show) -> None:
        """Process artwork for a TV show and its seasons."""
        try:
            self.stats['total_processed'] += 1
            year = str(show.year) if show.year else "Unknown Year"
            show_dir = self.base_output_dir / 'TV Shows' / self._sanitize_filename(f"{show.title} ({year})")
            
            # Add small delay between files
            time.sleep(0.1)
            
            # Download full quality show poster and background
            if show.posterUrl:
                # Remove any quality parameters to get original quality
                poster_url = show.posterUrl.split('?')[0]
                if not self._download_image(poster_url, show_dir / "poster.jpg"):
                    self.log_failure('show', show.title, year, "Failed to download poster", poster_url)
            
            time.sleep(0.1)
            
            if show.artUrl:
                # Remove any quality parameters to get original quality
                art_url = show.artUrl.split('?')[0]
                if not self._download_image(art_url, show_dir / "background.jpg"):
                    self.log_failure('show', show.title, year, "Failed to download background", art_url)
            
            # Process seasons
            for season in show.seasons():
                if season.index == 0:  # Skip specials
                    continue
                    
                # Create season folder
                season_dir = show_dir / f"Season {str(season.index).zfill(2)}"
                
                time.sleep(0.1)  # Add delay between season downloads
                
                # Download season poster
                if season.posterUrl:
                    # Remove any quality parameters to get original quality
                    season_poster_url = season.posterUrl.split('?')[0]
                    if not self._download_image(
                        season_poster_url,
                        season_dir / f"Season{str(season.index).zfill(2)}.jpg"
                    ):
                        self.log_failure('show', f"{show.title} - Season {season.index}", 
                                       year, "Failed to download season poster", season_poster_url)
                    
        except Exception as e:
            self.logger.error(f"Error processing show {show.title}: {e}")
            self.log_failure('show', show.title, year, str(e))

    def download_all_artwork(self) -> None:
        """Download artwork for all configured libraries."""
        try:
            start_time = time.time()
            
            # Process movies
            for library_name in self.config['libraries']['movies']:
                try:
                    library = self.plex.library.section(library_name)
                    all_items = library.all()
                    movies = [m for m in all_items if m.type == 'movie']  # Ensure we only get movies
                    
                    # Log detailed library information
                    self.logger.info(f"\nMovie Library Stats for {library_name}:")
                    self.logger.info(f"Total items in library: {len(all_items)}")
                    self.logger.info(f"Movies found: {len(movies)}")
                    self.logger.info(f"Difference: {len(all_items) - len(movies)} items")
                    
                    if not movies:
                        self.logger.warning(f"No movies found in library: {library_name}")
                        continue
                    
                    # Log any items that aren't movies
                    non_movies = [item for item in all_items if item.type != 'movie']
                    if non_movies:
                        self.logger.info(f"\nFound {len(non_movies)} non-movie items:")
                        for item in non_movies:
                            self.logger.info(f"- {item.title} (Type: {item.type})")
                    
                    self.logger.info(f"\nProcessing {len(movies)} movies from {library_name}")
                    
                    # Reduced number of concurrent downloads
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        list(tqdm(
                            executor.map(self.process_movie, movies),
                            total=len(movies),
                            desc=f"Downloading movie artwork from {library_name}"
                        ))
                except Exception as e:
                    self.logger.error(f"Error processing movie library {library_name}: {e}")

            # Process TV shows with similar detailed logging
            for library_name in self.config['libraries']['tv_shows']:
                try:
                    library = self.plex.library.section(library_name)
                    all_items = library.all()
                    shows = [s for s in all_items if s.type == 'show']
                    
                    self.logger.info(f"\nTV Library Stats for {library_name}:")
                    self.logger.info(f"Total items in library: {len(all_items)}")
                    self.logger.info(f"Shows found: {len(shows)}")
                    self.logger.info(f"Difference: {len(all_items) - len(shows)} items")
                    
                    if not shows:
                        self.logger.warning(f"No TV shows found in library: {library_name}")
                        continue
                    
                    # Log any items that aren't shows
                    non_shows = [item for item in all_items if item.type != 'show']
                    if non_shows:
                        self.logger.info(f"\nFound {len(non_shows)} non-show items:")
                        for item in non_shows:
                            self.logger.info(f"- {item.title} (Type: {item.type})")
                    
                    self.logger.info(f"\nProcessing {len(shows)} shows from {library_name}")
                    
                    with ThreadPoolExecutor(max_workers=2) as executor:
                        list(tqdm(
                            executor.map(self.process_show, shows),
                            total=len(shows),
                            desc=f"Downloading TV show artwork from {library_name}"
                        ))
                except Exception as e:
                    self.logger.error(f"Error processing TV show library {library_name}: {e}")
            
            # Print summary
            elapsed_time = time.time() - start_time
            self.logger.info("\nDownload Summary:")
            self.logger.info(f"Total items processed: {self.stats['total_processed']}")
            self.logger.info(f"Successful downloads: {self.stats['successful_downloads']}")
            self.logger.info(f"Failed downloads: {self.stats['failed_downloads']}")
            self.logger.info(f"Verification failures: {self.stats['verification_failures']}")
            self.logger.info(f"Total time elapsed: {elapsed_time:.2f} seconds")
            
            # Write error log
            self.write_error_log()
            if self.failed_items:
                self.logger.info(f"\nDetailed error log written to: {self.error_log_path}")
            
        except Exception as e:
            self.logger.error(f"Error during artwork download: {e}")
            raise
if __name__ == "__main__":
    downloader = PlexArtworkDownloader()
    downloader.download_all_artwork()