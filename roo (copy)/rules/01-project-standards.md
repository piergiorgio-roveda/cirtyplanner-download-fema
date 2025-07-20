# FEMA Flood Risk Data Collector - Project Standards

## Code Quality Standards
- Always use descriptive variable names that clearly indicate purpose
- Add comprehensive docstrings to all functions explaining parameters, return values, and purpose
- Include error handling for all external API calls and file operations
- Use type hints where appropriate for better code documentation

## Database Practices
- Always use parameterized queries to prevent SQL injection
- Create proper indexes for frequently queried columns
- Include foreign key constraints to maintain data integrity
- Log all database operations for debugging purposes

## API Integration Guidelines
- Implement proper rate limiting to respect external service limits
- Use exponential backoff for retry logic on failed requests
- Always validate API responses before processing
- Include comprehensive error logging with context

## File Organization
- Use consistent naming conventions: snake_case for Python files
- Organize scripts numerically (01_, 02_, etc.) to show execution order
- Keep configuration separate from code using config files
- Document all file paths and folder structures in README

## Data Processing Rules
- Always validate data before inserting into database
- Implement progress tracking for long-running operations
- Use batch processing for large datasets when possible
- Include data integrity checks and validation

## Configuration Management
- Use JSON configuration files for all configurable parameters
- Provide sensible defaults that work out of the box
- Document all configuration options in README
- Validate configuration on startup with clear error messages

## Error Handling
- Log errors with sufficient context for debugging
- Provide user-friendly error messages
- Implement graceful degradation where possible
- Include retry mechanisms for transient failures