import sys
import os

# Add the app directory to the path so we can import from it
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from dashboard import demo

if __name__ == "__main__":
    demo.launch()
