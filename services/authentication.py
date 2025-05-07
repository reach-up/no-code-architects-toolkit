# Copyright (c) 2025 Stephen G. Pope / AI Assistant
# Based on NCA Toolkit structure
# Licensed under GPL-2.0

import os
from functools import wraps
from flask import request, jsonify
import config # To access config.API_KEY loaded from environment
import logging

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def require_api_key(f):
    """
    Decorator to protect Flask routes based on the x-api-key header
    matching the API_KEY environment variable via config.py.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key')
        expected_key = getattr(config, 'API_KEY', None) # Get API_KEY from config module

        if not expected_key:
             logger.error("API_KEY is not configured in the environment/config.py. Denying all requests.")
             return jsonify({"error": "Configuration Error", "message": "API key is not configured on the server."}), 500

        # Check if the API key is provided and matches the one in config
        if api_key and api_key == expected_key:
            # If valid, proceed with the original function
            return f(*args, **kwargs)
        else:
            # If invalid or missing, return an unauthorized error
            log_msg = f"Unauthorized API access attempt to {request.path}."
            if not api_key:
                log_msg += " Missing x-api-key header."
            else:
                # Avoid logging the potentially sensitive key submitted by the user
                 log_msg += " Invalid API key provided."
            logger.warning(log_msg)
            return jsonify({"error": "Unauthorized", "message": "Invalid or missing API key."}), 401
    return decorated_function

# Add any other authentication-related functions below if needed in the future.
