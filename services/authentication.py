# Copyright (c) 2025 Stephen G. Pope / AI Assistant
# Based on NCA Toolkit structure
# Licensed under GPL-2.0

import os
from functools import wraps
from flask import request, jsonify
import config # Use config module consistently
import logging

# Setup logger (ensure logging is properly configured elsewhere, e.g., in app.py)
# If not configured elsewhere, basicConfig is okay, but might produce duplicate logs if app.py also configures it.
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s') # Example basic config

# Centralized error response
UNAUTHORIZED_RESPONSE = {"error": "Unauthorized", "message": "Invalid or missing API key."}
CONFIG_ERROR_RESPONSE = {"error": "Configuration Error", "message": "API key not configured on the server."}

# --- Original decorator - MODIFIED for consistency ---
def authenticate(func):
    """Authenticates requests using x-api-key header."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        api_key = request.headers.get('x-api-key') # Use lowercase
        expected_key = getattr(config, 'API_KEY', None)

        if not expected_key:
             logger.error("API_KEY is not configured. Denying request in @authenticate.")
             return jsonify(CONFIG_ERROR_RESPONSE), 500

        # Use expected_key from config
        if api_key and api_key == expected_key:
            return func(*args, **kwargs)
        else:
            logger.warning(f"Unauthorized API access attempt (authenticate) to {request.path}")
            return jsonify(UNAUTHORIZED_RESPONSE), 401
    return wrapper

# --- New decorator - Cleaned up ---
def require_api_key(f):
    """
    Decorator to protect Flask routes based on the x-api-key header
    matching the API_KEY environment variable via config.py. (Functionally same as authenticate)
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        api_key = request.headers.get('x-api-key') # Use lowercase
        expected_key = getattr(config, 'API_KEY', None)

        if not expected_key:
             logger.error("API_KEY is not configured. Denying request in @require_api_key.")
             return jsonify(CONFIG_ERROR_RESPONSE), 500

        if api_key and api_key == expected_key:
            return f(*args, **kwargs)
        else:
            log_msg = f"Unauthorized API access attempt (require_api_key) to {request.path}."
            if not api_key:
                log_msg += " Missing x-api-key header."
            else:
                 log_msg += " Invalid API key provided."
            logger.warning(log_msg)
            return jsonify(UNAUTHORIZED_RESPONSE), 401
    return decorated_function

# --- End ---