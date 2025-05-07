# Copyright (c) 2025 Stephen G. Pope, modifications by [Your Name/AI]
# Based on NCA Toolkit structure
# Licensed under GPL-2.0

import logging
import json
from flask import Blueprint, request, jsonify
from decorators import queue_task # Correct: Import from decorators module
from services.authentication import require_api_key # Correct: Import from authentication service
from services.v1.ffmpeg import video_assembly_service # Import the new service
import logging

# Setup logger
logger = logging.getLogger(__name__)
# Assuming logging is configured globally in app.py, otherwise uncomment below
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

v1_ffmpeg_compose_bp = Blueprint('v1_ffmpeg_compose_bp', __name__, url_prefix='/v1/ffmpeg')

# Placeholder for other potential ffmpeg tasks handled by this endpoint
def process_generic_ffmpeg_compose(data, job_id):
    logger.warning(f"Job {job_id}: Received generic ffmpeg/compose request, no specific handler implemented yet.")
    raise NotImplementedError("Generic ffmpeg/compose functionality not implemented yet.")


@v1_ffmpeg_compose_bp.route('/compose', methods=['POST'])
@require_api_key                 # Checks x-api-key header
@queue_task(bypass_queue=False)  # Handles async queuing if webhook_url present
def handle_ffmpeg_compose(job_id, data):
    """
    Handles complex FFmpeg operations based on payload.
    Routes to specific processors based on payload structure.
    Receives job_id and data from the queue_task decorator.
    """
    endpoint_name = '/v1/ffmpeg/compose'
    # Log only a snippet of data to avoid overly long log lines
    log_data_snippet = json.dumps(data)[:500] + ('...' if len(json.dumps(data)) > 500 else '')
    logger.info(f"Job {job_id}: Received request on {endpoint_name} with data snippet: {log_data_snippet}")

    try:
        # Ensure data is a dictionary (it might be None if request had no JSON body)
        if not isinstance(data, dict):
             logger.error(f"Job {job_id}: Invalid request data type: {type(data)}. Expected dict.")
             return "Invalid JSON payload received.", endpoint_name, 400

        inputs = data.get("inputs", {})
        if not isinstance(inputs, dict):
             logger.error(f"Job {job_id}: Invalid 'inputs' type in payload: {type(inputs)}. Expected dict.")
             return "Invalid JSON payload structure: 'inputs' must be an object.", endpoint_name, 400

        # --- Check if this is a Video Assembly Request ---
        is_video_assembly_request = (
            "image_sequence" in inputs and
            isinstance(inputs.get("image_sequence"), list) and
            "audio_input" in inputs and
            isinstance(inputs.get("audio_input"), dict) and
            "output_filename" in inputs
        )

        if is_video_assembly_request:
            # Check if the service was imported correctly (in case of issues)
            if video_assembly_service is None:
                 logger.error(f"Job {job_id}: video_assembly_service is not available. Module import might have failed.")
                 raise ImportError("Video assembly service module could not be imported.")

            logger.info(f"Job {job_id}: Detected Video Assembly request. Routing to video_assembly_service.")
            # Call the specific service function for video assembly
            # This function should handle the core logic (download, process, upload)
            # and return result details or raise exceptions on failure.
            result = video_assembly_service.process_video_assembly(data, job_id)
            logger.info(f"Job {job_id}: Video Assembly processing successful.")
            # The result from the service is returned (e.g., dict with gdrive_id)
            # This tuple format (result, endpoint, status) is expected by the queue processor in app.py
            return result, endpoint_name, 200
        else:
            # --- Handle other potential ffmpeg/compose tasks here ---
            logger.warning(f"Job {job_id}: Payload does not match Video Assembly structure. Generic compose not implemented.")
            raise NotImplementedError("Payload structure not recognized for implemented ffmpeg/compose operations.")
            # Example:
            # result = process_generic_ffmpeg_compose(data, job_id)
            # return result, endpoint_name, 200

    # --- Specific Error Handling ---
    except ValueError as ve: # Specific validation errors from services
        logger.error(f"Job {job_id}: Validation Error in {endpoint_name}: {ve}")
        return str(ve), endpoint_name, 400 # Bad Request
    except FileNotFoundError as fnfe:
         logger.error(f"Job {job_id}: File Not Found Error in {endpoint_name}: {fnfe}")
         return str(fnfe), endpoint_name, 404 # Not Found
    except ConnectionError as ce: # Specific connection/API errors (like GDrive)
         logger.error(f"Job {job_id}: Connection/API Error in {endpoint_name}: {ce}")
         # Use 502 Bad Gateway if it's an upstream service issue
         return str(ce), endpoint_name, 502
    except NotImplementedError as nie:
         logger.error(f"Job {job_id}: Not Implemented Error in {endpoint_name}: {nie}")
         return str(nie), endpoint_name, 501 # Not Implemented
    except ImportError as ie: # Catch potential import error for video service
         logger.error(f"Job {job_id}: Import Error: {ie}")
         return "Internal server error: Required module missing.", endpoint_name, 500
    # --- Catch-all Error Handling ---
    except Exception as e:
        logger.error(f"Job {job_id}: Unexpected error in {endpoint_name}: {e}", exc_info=True)
        # Return a generic error message and 500 status code
        return "Internal server error during media processing.", endpoint_name, 500