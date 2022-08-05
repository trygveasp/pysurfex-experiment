"""Set experiment path."""
import os
import sys

print("before", sys.path)
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)) + "/..")
print("after1", sys.path)
