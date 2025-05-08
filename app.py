# Copyright (c) 2025 Stephen G. Pope
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

import logging
import os
import time
import threading
import uuid
from flask import Flask, request
from queue import Queue
# send_webhook import is no longer needed if webhooks are fully removed
# from services.webhook import send_webhook 
from version import BUILD_NUMBER
from app_utils import log_job_status

# Setup logger if not configured globally elsewhere
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# logger = logging.getLogger(__name__)

# Read MAX_QUEUE_LENGTH from environment once
MAX_QUEUE_LENGTH = int(os.environ.get('MAX_QUEUE_LENGTH', 0))

def create_app():
    app = Flask(__name__)

    # --- In-memory Queue Setup ---
    task_queue = Queue()
    # Use a more descriptive name for clarity, maybe globally unique if needed?
    queue_id = f"queue_{id(task_queue)}"

    # Background thread function to process tasks from the queue
    def process_queue():
        logger = logging.getLogger(__name__ + ".process_queue") # Specific logger
        logger.info(f"Background task processor thread started for queue: {queue_id}")
        while True:
            job_id = "unknown_job" # Default in case get fails before job_id is assigned
            data = {} # Default data
            try:
                job_id, data, task_func_wrapper, queue_start_time = task_queue.get()
                logger.info(f"Job {job_id}: Dequeued task.")
                queue_time = time.time() - queue_start_time
                run_start_time = time.time()
                pid = os.getpid()

                log_job_status(job_id, {
                    "job_status": "running",
                    "job_id": job_id,
                    "queue_id": queue_id,
                    "process_id": pid,
                    "response": None
                })

                # Execute the actual task function provided by the decorator
                response_tuple = task_func_wrapper()
                # Expecting (result_data, endpoint_name, status_code)
                result_data, endpoint_name, status_code = response_tuple

                run_time = time.time() - run_start_time
                total_time = time.time() - queue_start_time

                # This dictionary is still useful for logging the job outcome
                job_outcome_details = {
                    "endpoint": endpoint_name,
                    "code": status_code,
                    "id": data.get("id"), # Original request ID from client
                    "job_id": job_id, # API generated Job ID
                    "response": result_data if status_code == 200 else None,
                    "message": "success" if status_code == 200 else result_data, # Assuming result_data is error message on failure
                    "pid": pid,
                    "queue_id": queue_id,
                    "run_time": round(run_time, 3),
                    "queue_time": round(queue_time, 3),
                    "total_time": round(total_time, 3),
                    "queue_length": task_queue.qsize(), # Length *after* task completion
                    "build_number": BUILD_NUMBER
                }

                log_job_status(job_id, {
                    "job_status": "done",
                    "job_id": job_id,
                    "queue_id": queue_id,
                    "process_id": pid,
                    "response": job_outcome_details # Log the detailed outcome
                })
                logger.info(f"Job {job_id}: Task processing finished with status code {status_code}.")

                # Webhook sending logic REMOVED
                # webhook_url = data.get("webhook_url")
                # if webhook_url:
                #    try:
                #        send_webhook(webhook_url, job_outcome_details) # Use job_outcome_details
                #        logger.info(f"Job {job_id}: Successfully sent webhook to {webhook_url}")
                #    except Exception as webhook_error:
                #         logger.error(f"Job {job_id}: Failed to send webhook to {webhook_url}: {webhook_error}", exc_info=True)

            except Exception as processing_error:
                # Log error if task_func_wrapper itself fails catastrophically
                # job_id might not be available if task_queue.get() failed, handle carefully
                current_job_id = job_id # job_id from outer scope if .get() succeeded
                if current_job_id == "unknown_job" and 'job_id' in locals() and locals()['job_id'] != "unknown_job": # Check if task_queue.get() populated it
                    current_job_id = locals()['job_id']
                
                logger.error(f"Job {current_job_id}: Error during task processing: {processing_error}", exc_info=True)
                try:
                     # Log failure status if possible
                     error_resp_data = {"error": "Processing Error", "message": str(processing_error)}
                     log_job_status(current_job_id, {
                         "job_status": "failed",
                         "job_id": current_job_id,
                         "queue_id": queue_id if 'queue_id' in locals() else 'unknown',
                         "process_id": pid if 'pid' in locals() else os.getpid(), # Get pid if not set
                         "response": error_resp_data
                     })
                     # Webhook sending on failure REMOVED
                     # if 'data' in locals() and data.get("webhook_url"):
                     #      failure_webhook_payload = {
                     #          "code": 500, "id": data.get("id"), "job_id": current_job_id,
                     #          "message": f"Internal server error during task processing: {type(processing_error).__name__}",
                     #          "response": None, "build_number": BUILD_NUMBER
                     #      }
                     #      send_webhook(data.get("webhook_url"), failure_webhook_payload)

                except Exception as inner_error:
                    logger.error(f"Job {current_job_id}: CRITICAL error during error logging for task: {processing_error} / Inner log error: {inner_error}", exc_info=True)

            finally:
                # Ensure task_done is called even if errors occur
                try:
                    task_queue.task_done()
                except Exception as td_error:
                     logger.error(f"Job {job_id if 'job_id' in locals() else 'unknown'}: Error calling task_done: {td_error}", exc_info=True)


    # Start the single background queue processing thread
    # daemon=True ensures thread exits when main app exits
    queue_processor_thread = threading.Thread(target=process_queue, daemon=True)
    queue_processor_thread.start()
    # --- End In-memory Queue Setup ---


    # --- Import and Register Blueprints ---
    # Original Routes (Legacy?)
    from routes.media_to_mp3 import convert_bp
    from routes.transcribe_media import transcribe_bp
    from routes.combine_videos import combine_bp
    from routes.audio_mixing import audio_mixing_bp
    from routes.gdrive_upload import gdrive_upload_bp
    from routes.authenticate import auth_bp
    from routes.caption_video import caption_bp
    from routes.extract_keyframes import extract_keyframes_bp
    from routes.image_to_video import image_to_video_bp

    app.register_blueprint(convert_bp)
    app.register_blueprint(transcribe_bp)
    app.register_blueprint(combine_bp)
    app.register_blueprint(audio_mixing_bp)
    app.register_blueprint(gdrive_upload_bp)
    app.register_blueprint(auth_bp)
    app.register_blueprint(caption_bp)
    app.register_blueprint(extract_keyframes_bp)
    app.register_blueprint(image_to_video_bp)


    # V1 Routes
    from routes.v1.ffmpeg.ffmpeg_compose import v1_ffmpeg_compose_bp
    from routes.v1.media.media_transcribe import v1_media_transcribe_bp
    from routes.v1.media.feedback import v1_media_feedback_bp, create_root_next_routes
    from routes.v1.media.convert.media_to_mp3 import v1_media_convert_mp3_bp
    from routes.v1.video.concatenate import v1_video_concatenate_bp
    from routes.v1.video.caption_video import v1_video_caption_bp
    from routes.v1.image.convert.image_to_video import v1_image_convert_video_bp
    from routes.v1.toolkit.test import v1_toolkit_test_bp
    from routes.v1.toolkit.authenticate import v1_toolkit_auth_bp
    from routes.v1.code.execute.execute_python import v1_code_execute_bp
    from routes.v1.s3.upload import v1_s3_upload_bp
    from routes.v1.video.thumbnail import v1_video_thumbnail_bp
    from routes.v1.media.download import v1_media_download_bp
    from routes.v1.media.convert.media_convert import v1_media_convert_bp
    from routes.v1.audio.concatenate import v1_audio_concatenate_bp
    from routes.v1.media.silence import v1_media_silence_bp
    from routes.v1.video.cut import v1_video_cut_bp
    from routes.v1.video.split import v1_video_split_bp
    from routes.v1.video.trim import v1_video_trim_bp
    from routes.v1.media.metadata import v1_media_metadata_bp
    from routes.v1.toolkit.job_status import v1_toolkit_job_status_bp
    from routes.v1.toolkit.jobs_status import v1_toolkit_jobs_status_bp

    app.register_blueprint(v1_ffmpeg_compose_bp)
    app.register_blueprint(v1_media_transcribe_bp)
    app.register_blueprint(v1_media_feedback_bp)
    app.register_blueprint(v1_media_convert_mp3_bp)
    app.register_blueprint(v1_video_concatenate_bp)
    app.register_blueprint(v1_video_caption_bp)
    app.register_blueprint(v1_image_convert_video_bp)
    app.register_blueprint(v1_toolkit_test_bp)
    app.register_blueprint(v1_toolkit_auth_bp)
    app.register_blueprint(v1_code_execute_bp)
    app.register_blueprint(v1_s3_upload_bp)
    app.register_blueprint(v1_video_thumbnail_bp)
    app.register_blueprint(v1_media_download_bp)
    app.register_blueprint(v1_media_convert_bp)
    app.register_blueprint(v1_audio_concatenate_bp)
    app.register_blueprint(v1_media_silence_bp)
    app.register_blueprint(v1_video_cut_bp)
    app.register_blueprint(v1_video_split_bp)
    app.register_blueprint(v1_video_trim_bp)
    app.register_blueprint(v1_media_metadata_bp)
    app.register_blueprint(v1_toolkit_job_status_bp)
    app.register_blueprint(v1_toolkit_jobs_status_bp)

    # Register a special route for Next.js root asset paths (if needed)
    create_root_next_routes(app)


    # --- Attach queue and config to app context ---
    # Make queue and config accessible for the decorator via app context
    app.nca_task_queue = task_queue
    app.config['MAX_QUEUE_LENGTH'] = MAX_QUEUE_LENGTH


    return app

# --- Create the Flask app instance ---
app = create_app()

# --- Run the app ---
if __name__ == '__main__':
    # Consider adding host and port configuration from env variables as well
    # For development, 0.0.0.0 makes it accessible externally
    app.run(host='0.0.0.0', port=8080) # debug=True should not be used in production
