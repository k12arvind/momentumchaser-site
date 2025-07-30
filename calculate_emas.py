#!/usr/bin/env python3
"""
Calculate and store EMAs for all existing data in the database
"""
import sys
import os
sys.path.append('src')

from database import update_all_emas
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Calculate EMAs for all symbols in the database"""
    
    logger.info("Starting EMA calculation for all symbols...")
    logger.info("This will calculate 4, 9, 18, 50, and 200-day EMAs")
    
    # Update all EMAs
    update_all_emas()
    
    logger.info("EMA calculation completed!")
    logger.info("All symbols now have EMA data available for the API and website")

if __name__ == "__main__":
    main()