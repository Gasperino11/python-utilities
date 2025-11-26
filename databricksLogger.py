import os
import logging
from typing import Optional
from datetime import datetime

class databricksLogger:
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
        timestamp_fmt: Optional[str] = None
    ):
        self.envr = envr
        
        # Set config format - use 'default' if not specified
        if config is not None:
            # Validate custom format string
            self._validate_format_string(config)
            self.config = config
        else:
            self.config = '[{asctime}][{envr}]:{message}'
        
        # Set timestamp format - use Python's strftime format
        if timestamp_fmt is not None:
            self.timestamp_fmt = timestamp_fmt
        else:
            self.timestamp_fmt = "%Y-%m-%d %H:%M:%S"
    
    def _validate_format_string(self, format_string: str) -> None:
        """
        Validate that the format string contains at least one of the required placeholders.
        
        Args:
            format_string: The format string to validate
        
        Raises:
            ValueError: If the format string doesn't contain at least one required placeholder
        """
        required_placeholders = ['{asctime}', '{message}', '{level}', '{envr}']
        
        if not any(placeholder in format_string for placeholder in required_placeholders):
            raise ValueError(
                f"Format string must contain at least one of: {', '.join(required_placeholders)}"
            )
    
    def _format_message(self, message: str, level: str) -> str:
        """
        Format the message with timestamp and color based on the configured format.
        
        Args:
            message: The log message
            level: The log level (INFO, WARNING, ERROR, SUCCESS, CRITICAL)
        
        Returns:
            The formatted and colored message
        """
        # Get current timestamp
        asctime = datetime.now().strftime(self.timestamp_fmt)
        
        # Format the message using the configured format string
        formatted = self.config.format(
            asctime=asctime,
            message=message,
            level=level,
            envr=self.envr
        )
        
        # Apply color
        color = self.COLORS.get(level, '')
        reset = self.COLORS['RESET']
        
        return f"{color}{formatted}{reset}"
    
    def info(self, message: str) -> None:
        """Log an info message."""
        colored_message = self._format_message(message, 'INFO')
        print(colored_message)
    
    def warning(self, message: str) -> None:
        """Log a warning message."""
        colored_message = self._format_message(message, 'WARNING')
        print(colored_message)
    
    def error(self, message: str) -> None:
        """Log an error message."""
        colored_message = self._format_message(message, 'ERROR')
        print(colored_message)
    
    def critical(self, message: str) -> None:
        """Log a critical message."""
        colored_message = self._format_message(message, 'CRITICAL')
        print(colored_message)
    
    def success(self, message: str) -> None:
        """Log a success message (custom level)."""
        colored_message = self._format_message(message, 'SUCCESS')
        print(colored_message)