# decorators.py
import os
import time
import uuid
from functools import wraps
from flask import request, current_app, jsonify
from queue import Queue
import config
from services.webhook import send_webhook
from app_utils import log_job_status # Verify this import path is correct for log_job_status
from version import BUILD_NUMBER
import logging

logger = logging.getLogger(__name__)

def queue_task(bypass_queue=False):
    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            try:
                task_queue = current_app.nca_task_queue
                max_queue_length = current_app.config.get('MAX_QUEUE_LENGTH', getattr(config, 'MAX_QUEUE_LENGTH', 0))
                queue_id = id(task_queue)
            except Exception as e:
                 logger.error(f"Error accessing app context for queue: {e}", exc_info=True)
                 return jsonify({"error": "Server Configuration Error", "message": "Task queue not available."}), 500

            job_id = str(uuid.uuid4())
            data = request.json if request.is_json else {}
            pid = os.getpid()
            start_time = time.time()
            webhook_url = data.get("webhook_url")
            should_bypass = bypass_queue

            if should_bypass:
                logger.info(f"Job {job_id}: Executing synchronously (bypass_queue={bypass_queue}, no_webhook={not webhook_url}).")
                try:
                    log_job_status(job_id, {"job_status": "running", "job_id": job_id, "queue_id": queue_id, "process_id": pid, "response": None})
                    result, endpoint_name, status_code = f(job_id=job_id, data=data, *args, **kwargs)
                    run_time = time.time() - start_time
                    response_obj = {
                        "code": status_code, "id": data.get("id"), "job_id": job_id,
                        "response": result if status_code == 200 else None,
                        "message": "success" if status_code == 200 else result,
                        "run_time": round(run_time, 3), "queue_time": 0, "total_time": round(run_time, 3),
                        "pid": pid, "queue_id": queue_id, "queue_length": task_queue.qsize(), "build_number": BUILD_NUMBER
                    }
                    log_job_status(job_id, {"job_status": "done", "job_id": job_id, "queue_id": queue_id, "process_id": pid, "response": response_obj})
                    return jsonify(response_obj), status_code
                except Exception as sync_error:
                    logger.error(f"Job {job_id}: Error during synchronous execution: {sync_error}", exc_info=True)
                    error_message = f"Internal server error during sync execution: {type(sync_error).__name__}"
                    error_obj = { "code": 500, "id": data.get("id"), "job_id": job_id, "message": error_message }
                    log_job_status(job_id, {"job_status": "failed", "job_id": job_id, "queue_id": queue_id, "process_id": pid, "response": error_obj})
                    return jsonify({"error": "Internal Server Error", "message": "Processing failed"}), 500
            else:
                logger.info(f"Job {job_id}: Queuing task for asynchronous execution.")
                if max_queue_length > 0 and task_queue.qsize() >= max_queue_length:
                    logger.warning(f"Job {job_id}: Queue is full (size={task_queue.qsize()}, max={max_queue_length}). Rejecting request.")
                    return jsonify({
                        "code": 429, "id": data.get("id"), "job_id": job_id,
                        "message": f"Task queue is full (MAX_QUEUE_LENGTH={max_queue_length}). Please try again later.",
                        "pid": pid, "queue_id": queue_id, "queue_length": task_queue.qsize(), "build_number": BUILD_NUMBER
                    }), 429
                log_job_status(job_id, {"job_status": "queued", "job_id": job_id, "queue_id": queue_id, "process_id": pid, "response": None})
                task_func_wrapper = lambda: f(job_id=job_id, data=data, *args, **kwargs)
                task_tuple = (job_id, data, task_func_wrapper, start_time)
                try:
                    task_queue.put(task_tuple)
                    logger.info(f"Job {job_id}: Task successfully added to the queue.")
                except Exception as queue_error:
                     logger.error(f"Job {job_id}: Failed to put task onto queue: {queue_error}", exc_info=True)
                     log_job_status(job_id, {"job_status": "failed", "job_id": job_id, "queue_id": queue_id, "process_id": pid, "response": {"error": "Queue Put Error"}})
                     return jsonify({"error": "Server Error", "message": "Failed to queue task."}), 500
                return jsonify({
                    "code": 202, "id": data.get("id"), "job_id": job_id, "message": "processing",
                    "pid": pid, "queue_id": queue_id, "max_queue_length": max_queue_length if max_queue_length > 0 else "unlimited",
                    "queue_length": task_queue.qsize(), "build_number": BUILD_NUMBER
                }), 202
        return wrapper
    return decorator