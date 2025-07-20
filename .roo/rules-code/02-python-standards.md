# Python Coding Standards for FEMA Project

## Python Style Guidelines
- Follow PEP 8 style guide for Python code
- Use 4 spaces for indentation (no tabs)
- Maximum line length of 88 characters (Black formatter standard)
- Use double quotes for strings unless single quotes avoid escaping

## Function Documentation
- All functions must have docstrings with parameters and return values
- Use Google-style docstrings for consistency
- Include examples for complex functions
- Document any exceptions that may be raised

## Import Organization
- Standard library imports first
- Third-party imports second
- Local application imports last
- Use absolute imports when possible
- Group imports logically and alphabetically within groups

## Variable Naming
- Use snake_case for variables and functions
- Use UPPER_CASE for constants
- Use descriptive names that explain purpose
- Avoid abbreviations unless they're widely understood

## Database Operations
- Always use context managers (with statements) for database connections
- Use parameterized queries to prevent SQL injection
- Include proper error handling for database operations
- Log database operations with appropriate detail level

## API Requests
- Always include timeout parameters for requests
- Implement proper retry logic with exponential backoff
- Use session objects for multiple requests to same host
- Include comprehensive error handling for network operations

## Configuration Handling
- Load configuration at startup and validate all required fields
- Provide clear error messages for missing or invalid configuration
- Use type hints for configuration parameters
- Document all configuration options

## Progress Tracking
- Implement progress indicators for long-running operations
- Use logging instead of print statements for production code
- Include timestamps in progress messages
- Provide meaningful progress updates (not just counters)

## Error Handling Best Practices
- Use specific exception types rather than generic Exception
- Log errors with full context and stack traces
- Provide user-friendly error messages
- Implement graceful degradation when possible
- Use try-except-finally blocks appropriately