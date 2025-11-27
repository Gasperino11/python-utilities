import os
import pytz
import logging
from datetime import datetime
from typing import Any, Optional
from databricks.sdk.runtime import spark, display, dbutils

class databricksLogger:
    """
    A lightweight, colorized logging utility for Databricks environments with timezone support.
    
    This logger provides colored console output with customizable formatting, timezone handling,
    and support for custom configuration values in log messages. It's designed to be simple yet
    flexible for development and production environments.
    
    **Features:**
    - ANSI color-coded output for different log levels
    - Configurable timestamp formats and timezones
    - Custom message formatting with template strings
    - Support for custom placeholders in log format
    - Lightweight implementation with minimal dependencies
    
    **Example Usage:**
    ```python
    # Basic usage with default settings (CST timezone)
    logger = databricksLogger(envr="prod")
    logger.info("Application started")
    logger.warning("Low memory warning")
    logger.error("Connection failed")
    
    # Custom configuration
    logger = databricksLogger(
        envr="dev",
        config="[{timestamp}][{level}][{envr}] {message}",
        timestamp_fmt="%Y-%m-%d %H:%M:%S.%f",
        timezone="America/New_York"
    )
    
    # With custom values
    logger = databricksLogger(
        envr="prod",
        config="[{timestamp}][{service}] {message}",
        custom_config_values={"service": "API"}
    )
    logger.info("Request processed")  # Output: [2025-11-26 14:30:00][API] Request processed
    ```
    
    **Attributes:**
        COLORS (dict): ANSI color codes for different log levels
    """
    
    # ANSI Color Codes
    COLORS = {
        'WARNING': '\033[33m',      # Yellow
        'INFO': '',                  # Default/Black (no color code)
        'ERROR': '\033[31m',         # Red
        'SUCCESS': '\033[32m',       # Green
        'CRITICAL': '\033[35m',      # Purple
        'RESET': '\033[0m'           # Reset to default
    }
    
    def __init__(
        self,
        envr: str,
        config: Optional[str] = None,
        custom_config_values: Optional[dict[str, Any]] = None,
        timestamp_fmt: Optional[str] = None,
        timezone: Optional[str] = None
    ):
        """
        Initialize the databricksLogger instance.
        
        **Parameters:**
            envr (str): Environment identifier (e.g., 'dev', 'prod', 'staging'). 
                This value is available as `{envr}` placeholder in the config string.
            config (Optional[str]): Custom log message format string using Python format syntax.
                Available placeholders: `{timestamp}`, `{message}`, `{level}`, `{envr}`, 
                and any keys from `custom_config_values`.
                Defaults to `'[{timestamp}]:{message}'`
            custom_config_values (Optional[dict[str, Any]]): Dictionary of custom key-value pairs
                to use as placeholders in the config string. For example, 
                `{'service': 'API', 'version': '1.0'}` allows using `{service}` and `{version}`
                in the config format string.
            timestamp_fmt (Optional[str]): Python strftime format string for timestamps.
                Defaults to `'%Y-%m-%d %H:%M:%S'`
                See: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
            timezone (Optional[str]): IANA timezone string (e.g., 'America/Chicago', 'UTC', 'Europe/London').
                Defaults to `'America/Chicago'` (CST)
        
        **Raises:**
            ValueError: If the custom config string doesn't contain at least one required placeholder
            pytz.UnknownTimeZoneError: If the provided timezone string is invalid
        
        **Example:**
            ```python
            logger = databricksLogger(
                envr="production",
                config="[{timestamp}][{level}][{service}] {message}",
                custom_config_values={"service": "DataPipeline"},
                timestamp_fmt="%Y-%m-%d %H:%M:%S",
                timezone="America/New_York"
            )
            ```
        """
        self.envr = envr
        
        # Set config format - use 'default' if not specified
        if config is not None:
            # Validate custom format string
            self._validate_format_string(config)
            self.config = config
        else:
            if envr.lower() in ['dev', 'development', 'test', 'testing']:
                self.config = '[{timestamp}] <{envr}> : {message}'
            else: 
                self.config = '[{timestamp}] : {message}'

        # Set custom dictionary for additional user-defined values in logging message
        if custom_config_values is not None:
            self.custom_config_values = custom_config_values
        else:
            self.custom_config_values = {}  
        
        # Set timestamp format - use Python's strftime format
        if timestamp_fmt is not None:
            self.timestamp_fmt = timestamp_fmt
        else:
            self.timestamp_fmt = "%Y-%m-%d %H:%M:%S"
        
        # Set timezone - default to America/Chicago (CST)
        if timezone is not None:
            self.timezone = pytz.timezone(timezone)
        else:
            self.timezone = pytz.timezone('America/Chicago')

        self.caching = False
        self.cached_logs = []
    
    def _validate_format_string(self, format_string: str) -> None:
        """
        Validate that the format string contains at least one required placeholder.
        
        This method ensures that custom format strings are valid and will produce meaningful output.
        At least one of the standard placeholders must be present in the format string.
        
        **Parameters:**
            format_string (str): The format string to validate
        
        **Raises:**
            ValueError: If the format string doesn't contain at least one of the required 
                placeholders: `{timestamp}`, `{message}`, `{level}`, or `{envr}`
        
        **Example:**
            ```python
            # Valid - contains {message}
            logger._validate_format_string("[{timestamp}] {message}")  # OK
            
            # Invalid - contains no required placeholders
            logger._validate_format_string("{custom}")  # Raises ValueError
            ```
        """
        required_placeholders = ['{timestamp}', '{message}', '{level}', '{envr}']
        
        if not any(placeholder in format_string for placeholder in required_placeholders):
            raise ValueError(
                f"Format string must contain at least one of: {', '.join(required_placeholders)}"
            )
    
    def _format_message(self, message: str, level: str, cache_message: bool = False) -> str:
        """
        Format the message with timestamp and color based on the configured format.
        
        This internal method combines all configuration (timestamp, timezone, custom values)
        with the provided message and log level to produce a formatted, color-coded output string.
        
        **Parameters:**
            message (str): The log message content
            level (str): The log level ('INFO', 'WARNING', 'ERROR', 'SUCCESS', 'CRITICAL')
        
        **Returns:**
            str: ANSI color-coded formatted message string ready for console output
        
        **Internal Process:**
            1. Generates timestamp in the configured timezone
            2. Formats the message using the configured format string
            3. Applies ANSI color codes based on log level
            4. Returns the formatted and colored string
        """
        # Get current timestamp in configured timezone
        timestamp = datetime.now(self.timezone).strftime(self.timestamp_fmt)
        
        # Format the message using the configured format string
        formatted = self.config.format(
            timestamp=timestamp,
            message=message,
            level=level,
            envr=self.envr,
            **(self.custom_config_values or {})
        )

        if self.caching and cache_message:
            log_entry = {
                "job_run_id": self.job_run_id,
                "timestamp": timestamp,
                "level": level,
                "envr": self.envr,
                "message": message
            }
            self.cached_logs.append(log_entry)
        
        # Apply color
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        
        return f"{color}{formatted}{reset}"
    
    def info(self, message: str, cache_message: bool = False) -> None:
        """
        Log an informational message.
        
        Outputs an info-level message with the configured format and no color coding.
        Optionally caches the message for later persistence to a Unity Catalog table.
        
        **Parameters:**
            message (str): The message to log
            cache_message (bool): If True, adds this message to the cache for later persistence.
                Defaults to False. Caching must be initialized via `init_caching()` first.
        
        **Raises:**
            Warning: If cache_message=True but caching is not initialized, logs a warning
        
        **Example:**
            ```python
            logger.info("User logged in successfully")
            logger.info("Processing started", cache_message=True)
            ```
        """
        colored_message = self._format_message(message, 'INFO', cache_message=cache_message)
        print(colored_message)
    
    def warning(self, message: str, cache_message: bool = False) -> None:
        """
        Log a warning message.
        
        Outputs a warning-level message in yellow (ANSI color code).
        Use for potentially problematic situations that don't prevent execution.
        Optionally caches the message for later persistence to a Unity Catalog table.
        
        **Parameters:**
            message (str): The message to log
            cache_message (bool): If True, adds this message to the cache for later persistence.
                Defaults to False. Caching must be initialized via `init_caching()` first.
        
        **Example:**
            ```python
            logger.warning("Cache miss for key: user_12345")
            logger.warning("Low memory detected", cache_message=True)
            ```
        """
        colored_message = self._format_message(message, 'WARNING', cache_message=cache_message)
        print(colored_message)
    
    def error(self, message: str, cache_message: bool = False) -> None:
        """
        Log an error message.
        
        Outputs an error-level message in red (ANSI color code).
        Use for serious problems that may prevent normal operation.
        Optionally caches the message for later persistence to a Unity Catalog table.
        
        **Parameters:**
            message (str): The message to log
            cache_message (bool): If True, adds this message to the cache for later persistence.
                Defaults to False. Caching must be initialized via `init_caching()` first.
        
        **Example:**
            ```python
            logger.error("Database connection failed: timeout after 30s")
            logger.error("Processing failed with error code 500", cache_message=True)
            ```
        """
        colored_message = self._format_message(message, 'ERROR', cache_message=cache_message)
        print(colored_message)
    
    def critical(self, message: str, cache_message: bool = False) -> None:
        """
        Log a critical message.
        
        Outputs a critical-level message in purple (ANSI color code).
        Use for critical errors that require immediate attention.
        Optionally caches the message for later persistence to a Unity Catalog table.
        
        **Parameters:**
            message (str): The message to log
            cache_message (bool): If True, adds this message to the cache for later persistence.
                Defaults to False. Caching must be initialized via `init_caching()` first.
        
        **Example:**
            ```python
            logger.critical("System disk space critically low: 5% remaining")
            logger.critical("Database unavailable", cache_message=True)
            ```
        """
        colored_message = self._format_message(message, 'CRITICAL', cache_message=cache_message)
        print(colored_message)
    
    def success(self, message: str, cache_message: bool = False) -> None:
        """
        Log a success message.
        
        Outputs a success-level message in green (ANSI color code).
        Custom log level useful for highlighting successful operations or milestones.
        Optionally caches the message for later persistence to a Unity Catalog table.
        
        **Parameters:**
            message (str): The message to log
            cache_message (bool): If True, adds this message to the cache for later persistence.
                Defaults to False. Caching must be initialized via `init_caching()` first.
        
        **Example:**
            ```python
            logger.success("Data pipeline completed: 50,000 records processed")
            logger.success("Job finished successfully", cache_message=True)
            ```
        """
        colored_message = self._format_message(message, 'SUCCESS', cache_message=cache_message)
        print(colored_message)

    def init_caching(self, uc_table_name: str) -> None:
        """
        Initialize log message caching for persistence to a Unity Catalog table.
        
        This method must be called before using the `cache_message=True` parameter in any logging method.
        It validates that the target table exists, retrieves the Databricks job run ID, and prepares
        the cache for storing log entries.
        
        **Parameters:**
            uc_table_name (str): The fully qualified Unity Catalog table name where logs will be persisted.
                Format: `<catalog>.<schema>.<table>` (e.g., `main.logs.job_logs`)
                The table must already exist and should have columns: job_run_id, envr, log_timestamp, level, message
        
        **Raises:**
            ValueError: If the specified table does not exist in the catalog
            ValueError: If job run ID cannot be determined from notebook context or job parameters
        
        **Side Effects:**
            - Sets `self.caching = True` if successful
            - Initializes `self.cached_logs = []` as an empty list
            - Retrieves and stores the Databricks job run ID in `self.job_run_id`
            - Stores reference to Spark session in `self.spark`
        
        **Example:**
            ```python
            logger = databricksLogger(envr="prod")
            
            # Initialize caching with the target table
            logger.init_caching("main.logs.job_logs")
            
            # Now log messages with caching enabled
            logger.info("Starting data processing", cache_message=True)
            logger.warning("Encountered retry scenario", cache_message=True)
            
            # Persist all cached logs to the table
            logger.persist_cache()
            ```
        """

        if not spark.catalog.tableExists(uc_table_name):
            raise ValueError(f"Table '{uc_table_name}' does not exist in the catalog.")

        self.uc_table_name = uc_table_name
        self.caching = True
        self.spark = spark
            
        try:
            self.info("Retrieving job run ID from Databricks notebook context.")
            self.job_run_id = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().get()
        except:
            self.warning("Unable to retrieve job run ID via Databricks notebook context, trying job parameters.")
            self.job_run_id = dbutils.widgets.getAll().get("job_id", None)
            if self.job_run_id is None:
                self.caching = False
                raise ValueError("Job run ID could not be determined from notebook context or fetched from job parameters; caching disabled.")

    def persist_cache(self) -> None:
        """
        Persist all cached log messages to the Unity Catalog table.
        
        This method writes all cached log entries (accumulated via `cache_message=True` in logging methods)
        to the target Unity Catalog table specified in `init_caching()`. Uses Spark's append mode with
        schema merging to ensure compatibility.
        
        **Raises:**
            ValueError: If caching has not been initialized via `init_caching()`
            PySpark exceptions: If the write operation fails (table permissions, data format, etc.)
        
        **Side Effects:**
            - Writes cached log entries to the Unity Catalog table
            - Clears `self.cached_logs` after successful persistence
            - Logs info and warning messages about the persistence operation
        
        **Returns:**
            None
        
        **Example:**
            ```python
            logger = databricksLogger(envr="prod")
            logger.init_caching("main.logs.job_logs")
            
            # Log messages throughout execution
            logger.info("Step 1 completed", cache_message=True)
            logger.info("Step 2 completed", cache_message=True)
            logger.warning("Minor issue encountered", cache_message=True)
            
            # Persist all cached logs at the end
            logger.persist_cache()
            ```
        """
        if not self.caching:
            raise ValueError("Caching is not enabled. Call 'init_caching' first.")
            
        if len(self.cached_logs) == 0:
            self.critical("No cached logs to persist.")
        else:
            self.info(f"Persisting {len(self.cached_logs)} cached log entries to table '{self.uc_table_name}'.")
            df = self.spark.createDataFrame(self.cached_logs)
            df.write.mode("append").option("mergeSchema", "true").saveAsTable(self.uc_table_name)
            self.info("Cached logs persisted successfully; flushing current cache.")
            self.cached_logs = []