# Replace content of services/v1/ffmpeg/video_assembly_service.py

import os
import subprocess
import logging
import tempfile
import shutil
from services import gdrive_service # Keep for potential GDrive use
from services import s3_toolkit # Use S3 toolkit
import config

logger = logging.getLogger(__name__)
# Assuming logging is configured globally in app.py
# logging.basicConfig(level=logging.INFO) # Remove if configured globally

def generate_ffmpeg_command(image_inputs, audio_path, output_path, ffmpeg_options):
    # --- This function remains the same ---
    command = ["ffmpeg"]
    filter_complex_parts = []
    input_mappings = []
    for i, img_input in enumerate(image_inputs):
        command.extend(["-loop", "1", "-t", str(img_input['duration']), "-i", img_input['path']])
        filter_complex_parts.append(f"[{i}:v]settb=AVTB,setpts=PTS-STARTPTS,fps=fps=25,format=pix_fmts=yuv420p[v{i}];")
        input_mappings.append(f"[v{i}]")
    command.extend(["-i", audio_path])
    audio_input_index = len(image_inputs)
    filter_complex = "".join(filter_complex_parts)
    filter_complex += "".join(input_mappings)
    filter_complex += f"concat=n={len(image_inputs)}:v=1:a=0[outv]"
    command.extend(["-filter_complex", filter_complex])
    command.extend(["-map", "[outv]"])
    command.extend(["-map", f"{audio_input_index}:a?"])
    if ffmpeg_options.get("video_codec"): command.extend(["-c:v", ffmpeg_options["video_codec"]])
    if ffmpeg_options.get("tune"): command.extend(["-tune", ffmpeg_options["tune"]])
    if ffmpeg_options.get("audio_codec"): command.extend(["-c:a", ffmpeg_options["audio_codec"]])
    if ffmpeg_options.get("audio_bitrate"): command.extend(["-b:a", ffmpeg_options["audio_bitrate"]])
    if ffmpeg_options.get("fps_mode"): command.extend(["-fps_mode", ffmpeg_options["fps_mode"]])
    command.extend(["-pix_fmt", "yuv420p"])
    if "other_flags" in ffmpeg_options: command.extend(ffmpeg_options["other_flags"])
    command.append(output_path)
    logger.info(f"Generated FFmpeg command: {' '.join(command)}")
    return command
    # --- End generate_ffmpeg_command ---

def process_video_assembly(data, job_id):
    inputs = data.get("inputs", {}); outputs_spec = data.get("outputs", [])
    if not inputs or not outputs_spec: raise ValueError("Missing 'inputs' or 'outputs'.")
    output_filename = inputs.get("output_filename", f"video_assembly_{job_id}.mp4")
    audio_input = inputs.get("audio_input"); image_sequence = inputs.get("image_sequence"); ffmpeg_options = inputs.get("ffmpeg_options", {})
    output_target = outputs_spec[0]; output_type = output_target.get("type", "s3")
    if not audio_input or not image_sequence: raise ValueError("Missing audio_input or image_sequence.")

    default_s3_bucket = getattr(config, 'S3_BUCKET_NAME', None)
    # +++ Debug Log 1 +++
    logger.info(f"Job {job_id}: Inside process_video_assembly - Default S3 bucket from config: '{default_s3_bucket}' (Type: {type(default_s3_bucket)})")

    temp_dir = tempfile.mkdtemp(prefix=f"nca_{job_id}_", dir=config.LOCAL_STORAGE_PATH)
    logger.info(f"Job {job_id}: Created temp dir: {temp_dir}")
    try:
        local_audio_path = None; image_inputs_for_ffmpeg = []; download_tasks = []
        audio_source_type = audio_input.get("source_type")

        if audio_source_type == "gdrive_id" and audio_input.get("file_id"):
             audio_id = audio_input['file_id']; ext = os.path.splitext(audio_input.get("file_name", "audio.mp3"))[1] or ".mp3"
             local_audio_path = os.path.join(temp_dir, f"audio_{audio_id}{ext}")
             download_tasks.append({"type": "gdrive", "id": audio_id, "path": local_audio_path})
        elif audio_source_type == "s3_object_key" and audio_input.get("object_key"):
             obj_key = audio_input['object_key']; bucket = audio_input.get("bucket_name", default_s3_bucket)
             # +++ Debug Log 2 +++
             logger.info(f"Job {job_id}: Audio task - Bucket value determined: '{bucket}' (Type: {type(bucket)})")
             ext = os.path.splitext(audio_input.get("file_name", os.path.basename(obj_key)))[1] or ".tmp"
             local_audio_path = os.path.join(temp_dir, f"audio{ext}")
             download_tasks.append({"type": "s3", "bucket": bucket, "key": obj_key, "path": local_audio_path})
        else: raise ValueError("Invalid audio_input source_type/keys.")

        for i, img_data in enumerate(image_sequence):
            img_source_type = img_data.get("source_type"); duration = img_data.get("duration")
            if duration is None: raise ValueError(f"Missing duration for image {i}.")
            local_img_path = None; img_task = None; ext = ".tmp"; original_name = img_data.get("file_name", f"image{i}")
            if '.' in original_name: name_part, ext_part = os.path.splitext(original_name); ext = ext_part if ext_part else '.tmp'

            if img_source_type == "gdrive_id" and img_data.get("file_id"):
                 img_id = img_data['file_id']; local_img_path = os.path.join(temp_dir, f"image_{i}_{img_id}{ext}"); img_task = {"type": "gdrive", "id": img_id, "path": local_img_path}
            elif img_source_type == "s3_object_key" and img_data.get("object_key"):
                 obj_key = img_data['object_key']; bucket = img_data.get("bucket_name", default_s3_bucket)
                 # +++ Debug Log 3 +++
                 logger.info(f"Job {job_id}: Image task {i} - Bucket value determined: '{bucket}' (Type: {type(bucket)})")
                 local_img_path = os.path.join(temp_dir, f"image_{i}{ext}")
                 img_task = {"type": "s3", "bucket": bucket, "key": obj_key, "path": local_img_path}
            else: raise ValueError(f"Invalid image {i} source_type/keys.")
            download_tasks.append(img_task); image_inputs_for_ffmpeg.append({'path': local_img_path, 'duration': duration})

        # +++ Debug Log 4 +++
        logger.info(f"Job {job_id}: Final download_tasks list prepared: {download_tasks}")

        logger.info(f"Job {job_id}: Starting downloads for {len(download_tasks)} files...")
        for task_index, task in enumerate(download_tasks):
            # +++ Debug Log 5 +++
            logger.info(f"Job {job_id}: Processing Task {task_index}: {task}")
            task_bucket = task.get('bucket') # Get bucket value safely
            logger.info(f"Job {job_id}: Task {task_index} bucket value from get: '{task_bucket}' (Type: {type(task_bucket)})")

            logger.info(f"Job {job_id}: Downloading {task['type']} source: {task.get('id') or task.get('key')}")
            if task['type'] == 'gdrive':
                 gdrive_service.download_gdrive_file(task['id'], task['path'])
            elif task['type'] == 's3':
                 # Check the variable we logged
                 if not task_bucket:
                      logger.error(f"Job {job_id}: Raising ValueError because task_bucket ('{task_bucket}') is falsey.")
                      raise ValueError("Missing bucket name for S3 download.")
                 s3_toolkit.download_file_from_s3(task_bucket, task['key'], task['path'])
            logger.info(f"Job {job_id}: Downloaded to {task['path']}")
        logger.info(f"Job {job_id}: All downloads complete.")

        logger.info(f"Job {job_id}: Generating FFmpeg command...")
        local_output_path = os.path.join(temp_dir, output_filename)
        ffmpeg_command = generate_ffmpeg_command(image_inputs_for_ffmpeg, local_audio_path, local_output_path, ffmpeg_options)

        logger.info(f"Job {job_id}: Executing FFmpeg command...")
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)
        if result.returncode != 0: logger.error(f"Job {job_id}: FFmpeg failed. Stderr:\n{result.stderr}"); raise Exception(f"FFmpeg failed: {result.stderr}")
        else: logger.info(f"Job {job_id}: FFmpeg successful."); logger.info(f"Job {job_id}: Output video at {local_output_path}")

        final_output_details = {}
        if output_type == "gdrive":
             gdrive_folder_id = output_target.get("folder_id");
             if not gdrive_folder_id: raise ValueError("Missing folder_id for gdrive output.")
             logger.info(f"Job {job_id}: Uploading to GDrive folder {gdrive_folder_id}...")
             uploaded_info = gdrive_service.upload_file_to_gdrive(local_output_path, gdrive_folder_id, output_filename)
             logger.info(f"Job {job_id}: GDrive upload successful. ID: {uploaded_info.get('id')}")
             final_output_details = {"gdrive_id": uploaded_info.get('id'), "filename": uploaded_info.get('name'), "url": uploaded_info.get('webViewLink'), "storage_type": "gdrive"}
        elif output_type == "s3":
             output_bucket = output_target.get("bucket_name", default_s3_bucket); output_prefix = output_target.get("object_key_prefix", f"youtube/{job_id}/")
             output_key = os.path.join(output_prefix, output_filename).replace("\\", "/")
             if not output_bucket: raise ValueError("Missing bucket_name for S3 output.")
             logger.info(f"Job {job_id}: Uploading to S3 bucket {output_bucket} as {output_key}...")
             uploaded_info = s3_toolkit.upload_file_to_s3(local_output_path, output_bucket, output_key)
             logger.info(f"Job {job_id}: S3 upload successful.")
             final_output_details = {"s3_bucket": uploaded_info.get('bucket'), "s3_key": uploaded_info.get('object_key'), "url": uploaded_info.get('url'), "storage_type": "s3"}
        else: raise ValueError(f"Unsupported output type: {output_type}")
        return final_output_details
    except Exception as e: logger.error(f"Job {job_id}: Error during video assembly: {e}", exc_info=True); raise
    finally:
        logger.info(f"Job {job_id}: Cleaning up temp directory: {temp_dir}")
        if os.path.exists(temp_dir):
            try: shutil.rmtree(temp_dir); logger.info(f"Job {job_id}: Temp directory cleaned up.")
            except Exception as ce: logger.error(f"Job {job_id}: Error cleaning up {temp_dir}: {ce}", exc_info=True)