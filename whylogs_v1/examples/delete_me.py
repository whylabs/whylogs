#!/usr/bin/env python3
"""Fibonacci numbers"""
from typing import List


def user_input() -> str:
    """Accept user input for decrement function
    ---
    - Accept a number from user input.
    - Throw exception if user inputs non-numeric.
    """
    try:
        n = input("Please enter a number: ")
        return (
            f"Decrement number {n} is: {f(int(n))}."
        )
    except Exception as e:
        raise e


def f(n: int) -> int:
    """Return n decremented."""
    return n - 1


if __name__ == "__main__":
    print(user_input())
