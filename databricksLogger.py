import os
import pytz
import logging
from datetime import datetime
from typing import Any, Optional

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
    
    def _format_message(self, message: str, level: str) -> str:
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
        
        # Apply color
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        
        return f"{color}{formatted}{reset}"
    
    def info(self, message: str) -> None:
        """
        Log an informational message.
        
        Outputs an info-level message with the configured format and no color coding.
        
        **Parameters:**
            message (str): The message to log
        
        **Example:**
            ```python
            logger.info("User logged in successfully")
            ```
        """
        colored_message = self._format_message(message, 'INFO')
        print(colored_message)
    
    def warning(self, message: str) -> None:
        """
        Log a warning message.
        
        Outputs a warning-level message in yellow (ANSI color code).
        Use for potentially problematic situations that don't prevent execution.
        
        **Parameters:**
            message (str): The message to log
        
        **Example:**
            ```python
            logger.warning("Cache miss for key: user_12345")
            ```
        """
        colored_message = self._format_message(message, 'WARNING')
        print(colored_message)
    
    def error(self, message: str) -> None:
        """
        Log an error message.
        
        Outputs an error-level message in red (ANSI color code).
        Use for serious problems that may prevent normal operation.
        
        **Parameters:**
            message (str): The message to log
        
        **Example:**
            ```python
            logger.error("Database connection failed: timeout after 30s")
            ```
        """
        colored_message = self._format_message(message, 'ERROR')
        print(colored_message)
    
    def critical(self, message: str) -> None:
        """
        Log a critical message.
        
        Outputs a critical-level message in purple (ANSI color code).
        Use for critical errors that require immediate attention.
        
        **Parameters:**
            message (str): The message to log
        
        **Example:**
            ```python
            logger.critical("System disk space critically low: 5% remaining")
            ```
        """
        colored_message = self._format_message(message, 'CRITICAL')
        print(colored_message)
    
    def success(self, message: str) -> None:
        """
        Log a success message.
        
        Outputs a success-level message in green (ANSI color code).
        Custom log level useful for highlighting successful operations or milestones.
        
        **Parameters:**
            message (str): The message to log
        
        **Example:**
            ```python
            logger.success("Data pipeline completed: 50,000 records processed")
            ```
        """
        colored_message = self._format_message(message, 'SUCCESS')
        print(colored_message)
    
    @staticmethod
    def display_table(data: list, headers: Optional[list[str]] = None) -> None:
        """
        Display data as a markdown table in a Databricks cell.
        
        This static method formats user-provided data as a markdown table and displays it
        using Databricks' `display()` function. Supports both list of dictionaries and 
        list of lists as input formats.
        
        **Parameters:**
            data (list): The data to display. Can be one of:
                - List of dictionaries: `[{'name': 'Alice', 'age': 30}, {'name': 'Bob', 'age': 25}]`
                - List of lists: `[['Alice', 30], ['Bob', 25]]`
            headers (Optional[list[str]]): Column headers for the table. 
                Required when data is a list of lists.
                Optional when data is a list of dictionaries (will use dict keys as headers).
                If provided with dict data, these headers will be used instead of the keys.
        
        **Raises:**
            ValueError: If data is empty
            ValueError: If headers are required but not provided
            ValueError: If headers length doesn't match data row length
            TypeError: If data format is not supported (must be list of dicts or list of lists)
        
        **Example:**
            ```python
            # Using list of dictionaries
            employees = [
                {'name': 'Alice', 'department': 'Engineering', 'salary': 120000},
                {'name': 'Bob', 'department': 'Sales', 'salary': 90000},
                {'name': 'Charlie', 'department': 'Engineering', 'salary': 115000}
            ]
            databricksLogger.display_table(employees)
            
            # Using list of lists with headers
            data = [
                ['Product A', 1500, 45],
                ['Product B', 2300, 32],
                ['Product C', 1800, 58]
            ]
            headers = ['Product', 'Revenue', 'Units Sold']
            databricksLogger.display_table(data, headers=headers)
            
            # Using list of lists, overriding dict keys
            employees = [
                {'name': 'Alice', 'department': 'Engineering'},
                {'name': 'Bob', 'department': 'Sales'}
            ]
            custom_headers = ['Employee Name', 'Dept']
            databricksLogger.display_table(employees, headers=custom_headers)
            ```
        """
        if not data:
            raise ValueError("Data cannot be empty")
        
        # Determine if data is list of dicts or list of lists
        is_dict_data = isinstance(data[0], dict)
        
        if is_dict_data:
            # Extract headers from dict keys if not provided
            if headers is None:
                headers = list(data[0].keys())
            
            # Validate headers match dict keys
            if len(headers) != len(data[0]):
                raise ValueError(f"Headers length ({len(headers)}) doesn't match data keys ({len(data[0])})")
            
            # Convert dicts to list of lists using headers
            rows = [[row.get(header, '') for header in headers] for row in data]
        
        elif isinstance(data[0], (list, tuple)):
            # List of lists/tuples
            if headers is None:
                raise ValueError("Headers are required when data is a list of lists")
            
            rows = data
            
            # Validate headers match row length
            if len(headers) != len(rows[0]):
                raise ValueError(f"Headers length ({len(headers)}) doesn't match row length ({len(rows[0])})")
        
        else:
            raise TypeError(f"Data must be a list of dicts or list of lists, got {type(data[0])}")
        
        # Build markdown table
        markdown_table = "| " + " | ".join(str(h) for h in headers) + " |\n"
        markdown_table += "|" + "|".join(["---"] * len(headers)) + "|\n"
        
        for row in rows:
            markdown_table += "| " + " | ".join(str(cell) for cell in row) + " |\n"
        
        # Display using Databricks display function
        display(markdown_table)