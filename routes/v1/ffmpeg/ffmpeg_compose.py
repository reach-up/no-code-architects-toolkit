# Copyright (c) 2025 Stephen G. Pope, modifications by [Your Name/AI]
# Based on NCA Toolkit structure
# Licensed under GPL-2.0

import logging
import json
from flask import Blueprint, request, jsonify
from services.authentication import require_api_key
from app import queue_task # Import the queue task decorator

# --- Added for Video Assembly Feature ---
# Import the new video assembly service
from services.v1.ffmpeg import video_assembly_service
# --- End Added for Video Assembly Feature ---

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

v1_ffmpeg_compose_bp = Blueprint('v1_ffmpeg_compose_bp', __name__, url_prefix='/v1/ffmpeg')

# --- Placeholder for potential existing FFMPEG Compose Logic ---
# If you had other logic here previously, you would merge it or keep it
# accessible, perhaps via different checks within the handle_ffmpeg_compose function.
def process_generic_ffmpeg_compose(data, job_id):
    logger.warning(f"Job {job_id}: Received generic ffmpeg/compose request, no specific handler implemented yet.")
    raise NotImplementedError("Generic ffmpeg/compose functionality not implemented yet.")
# --- End Placeholder ---


@v1_ffmpeg_compose_bp.route('/compose', methods=['POST'])
@require_api_key
@queue_task(bypass_queue=False) # Use the queue if webhook_url is present
def handle_ffmpeg_compose(job_id, data):
    """
    Handles complex FFmpeg operations based on payload.
    Routes to specific processors based on payload structure.
    """
    endpoint_name = '/v1/ffmpeg/compose'
    logger.info(f"Job {job_id}: Received request on {endpoint_name} with data: {json.dumps(data)[:500]}...") # Log snippet

    try:
        inputs = data.get("inputs", {})

        # --- Added for Video Assembly Feature ---
        # Check if this is a Video Assembly Request based on expected keys
        is_video_assembly_request = (
            "image_sequence" in inputs and
            isinstance(inputs["image_sequence"], list) and
            "audio_input" in inputs and
            isinstance(inputs["audio_input"], dict) and
            "output_filename" in inputs
        )

        if is_video_assembly_request:
            logger.info(f"Job {job_id}: Detected Video Assembly request. Routing to video_assembly_service.")
            # Call the specific service function for video assembly
            result = video_assembly_service.process_video_assembly(data, job_id)
            logger.info(f"Job {job_id}: Video Assembly processing successful.")
            # The result from the service is returned, which will be used in the webhook
            return result, endpoint_name, 200
        # --- End Added for Video Assembly Feature ---
        else:
            # ** Handle other potential ffmpeg/compose tasks here **
            # If you have other logic that uses this endpoint, add checks for it here.
            # Otherwise, raise the NotImplementedError.
            logger.warning(f"Job {job_id}: Payload does not match Video Assembly structure. Generic compose not implemented.")
            raise NotImplementedError("Payload structure not recognized for implemented ffmpeg/compose operations.")
            # Example: Call a generic processor if needed
            # result = process_generic_ffmpeg_compose(data, job_id)
            # return result, endpoint_name, 200

    except ValueError as ve: # Specific validation errors from services
        logger.error(f"Job {job_id}: Validation Error in {endpoint_name}: {ve}")
        return str(ve), endpoint_name, 400 # Bad Request
    except FileNotFoundError as fnfe: # Specific file not found error
         logger.error(f"Job {job_id}: File Not Found Error in {endpoint_name}: {fnfe}")
         return str(fnfe), endpoint_name, 404 # Not Found
    except ConnectionError as ce: # Specific connection/API errors (like GDrive)
         logger.error(f"Job {job_id}: Connection/API Error in {endpoint_name}: {ce}")
         return str(ce), endpoint_name, 502 # Bad Gateway (or appropriate status)
    except NotImplementedError as nie:
         logger.error(f"Job {job_id}: Not Implemented Error in {endpoint_name}: {nie}")
         return str(nie), endpoint_name, 501 # Not Implemented
    except Exception as e:
        # Catch-all for other unexpected errors during processing
        logger.error(f"Job {job_id}: Unexpected error in {endpoint_name}: {e}", exc_info=True)
        # Ensure a generic error message is returned to avoid leaking details
        return "Internal server error during media processing.", endpoint_name, 500

