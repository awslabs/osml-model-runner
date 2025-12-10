# Code Conventions

## Docstring Format

This project uses **Sphinx docstring format** for all documentation.

### Sphinx Directives

Use the following directives in docstrings:

- `:param param_name:` - Document function/method parameters
- `:raises ExceptionType:` - Document exceptions that may be raised
- `:return:` - Document return values

### Important: Do NOT Use Type Directives

**Do not include** `:type:` or `:rtype:` directives in docstrings. The project uses `sphinx-autodoc-typehints` which automatically extracts type information from Python type annotations.

### Correct Example

```python
def process_data(
    data: str,
    max_retries: int,
    timeout: Optional[float] = None,
) -> bool:
    """
    Process the input data with retry logic.
    
    :param data: The data string to process
    :param max_retries: Maximum number of retry attempts
    :param timeout: Optional timeout in seconds for the operation
    :return: True if processing succeeded, False otherwise
    :raises ValueError: If data is empty or max_retries is negative
    :raises TimeoutError: If operation exceeds the timeout
    """
```

### Incorrect Example (Do Not Use)

```python
def process_data(
    data: str,
    max_retries: int,
    timeout: Optional[float] = None,
) -> bool:
    """
    Process the input data with retry logic.
    
    :param data: The data string to process
    :type data: str  # ❌ DO NOT INCLUDE - redundant with type hints
    :param max_retries: Maximum number of retry attempts
    :type max_retries: int  # ❌ DO NOT INCLUDE - redundant with type hints
    :param timeout: Optional timeout in seconds for the operation
    :type timeout: Optional[float]  # ❌ DO NOT INCLUDE - redundant with type hints
    :return: True if processing succeeded, False otherwise
    :rtype: bool  # ❌ DO NOT INCLUDE - redundant with type hints
    :raises ValueError: If data is empty or max_retries is negative
    :raises TimeoutError: If operation exceeds the timeout
    """
```

## Docstring Structure

### Module Docstrings

Place at the top of each module file:

```python
"""
Brief one-line description of the module.

Optional longer description providing more context about the module's purpose,
key classes, and functionality.
"""
```

### Class Docstrings

```python
class MyClass:
    """
    Brief one-line description of the class.
    
    Optional longer description explaining the class purpose, behavior,
    and usage patterns.
    """
```

### Method/Function Docstrings

```python
def my_function(param1: str, param2: int) -> bool:
    """
    Brief one-line description of what the function does.
    
    Optional longer description providing additional context, algorithm
    details, or usage notes.
    
    :param param1: Description of first parameter
    :param param2: Description of second parameter
    :return: Description of return value
    :raises ValueError: When param2 is negative
    :raises RuntimeError: When operation fails
    """
```

## Type Annotations

- **Always** include type annotations for function parameters and return values
- Use `typing` module for complex types (`Optional`, `List`, `Dict`, etc.)
- Type annotations are the source of truth for documentation - sphinx-autodoc-typehints will extract them automatically

## Code Style and Linting

This project uses automated tools to enforce code style and quality standards.

### Running Linters

Use tox to run all linting checks:

```bash
tox -e lint
```

This runs pre-commit hooks that check code formatting, style, and quality.

### Pre-commit Hooks

The project uses the following tools (configured in `.pre-commit-config.yaml`):

- **black** - Code formatter with 125 character line length
- **isort** - Import statement organizer (black-compatible profile)
- **flake8** - Style guide enforcement (ignores E203, W503, W605)
- **autopep8** - Additional PEP 8 formatting
- **pre-commit-hooks** - Basic checks (trailing whitespace, YAML validation, end-of-file fixer)
- **copyright** - Ensures copyright headers are present and up-to-date

### Manual Formatting

To manually format code before committing:

```bash
# Format with black
black --line-length=125 path/to/file.py

# Sort imports
isort --line-length=125 --profile=black path/to/file.py

# Check with flake8
flake8 --ignore=E203,W503,W605 --max-line-length=125 path/to/file.py
```

### Installing Pre-commit Hooks

To automatically run checks before each commit:

```bash
pip install pre-commit
pre-commit install
```

After installation, the hooks will run automatically on `git commit`.

## General Guidelines

- Keep docstrings concise but informative
- Use complete sentences with proper punctuation
- Document all public APIs (classes, functions, methods)
- Private methods (prefixed with `_`) should have docstrings for complex logic
- Include examples in docstrings when behavior is non-obvious
- Follow PEP 8 style guidelines with 125 character line length
- Run `tox -e lint` before submitting code to catch style issues early
