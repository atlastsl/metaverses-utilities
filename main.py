#!/usr/bin/env python3
"""
Simple Python Script Project
"""

import os
import sys
from typing import Optional


def greet_user(name: Optional[str] = None) -> str:
    """Greet the user with a personalized message."""
    if name is None:
        name = "World"
    return f"Hello, {name}!"


def calculate_sum(numbers: list[int]) -> int:
    """Calculate the sum of a list of numbers."""
    return sum(numbers)


def main() -> None:
    """Main function to run the script."""
    print("=== Simple Python Script ===")
    
    # Example usage of functions
    print(greet_user())
    print(greet_user("Python Developer"))
    
    # Calculate sum of numbers
    numbers = [1, 2, 3, 4, 5]
    result = calculate_sum(numbers)
    print(f"Sum of {numbers} is: {result}")
    
    print("Script completed successfully!")


if __name__ == "__main__":
    main()