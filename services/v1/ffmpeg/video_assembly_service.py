import os
import subprocess
import logging
import tempfile
import shutil
# Import both services
from services import gdrive_service
from services import s3_toolkit # Use S3 toolkit
import config

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def generate_ffmpeg_command(image_inputs, audio_path, output_path, ffmpeg_options):
    command = ["ffmpeg"]
    filter_complex_parts = []
    input_mappings = []
    for i, img_input in enumerate(image_inputs):
        command.extend(["-loop", "1", "-t", str(img_input['duration']), "-i", img_input['path']])
        # Added explicit framerate filter for compatibility
        filter_complex_parts.append(f"[{i}:v]settb=AVTB,setpts=PTS-STARTPTS,fps=fps=25,format=pix_fmts=yuv420p[v{i}];")
        input_mappings.append(f"[v{i}]")
    command.extend(["-i", audio_path])
    audio_input_index = len(image_inputs)
    filter_complex = "".join(filter_complex_parts)
    filter_complex += "".join(input_mappings)
    # Removed format filter here, added above per stream and one for output
    filter_complex += f"concat=n={len(image_inputs)}:v=1:a=0[outv]"
    command.extend(["-filter_complex", filter_complex])
    command.extend(["-map", "[outv]"])
    command.extend(["-map", f"{audio_input_index}:a?"])
    if ffmpeg_options.get("video_codec"):
        command.extend(["-c:v", ffmpeg_options["video_codec"]])
    if ffmpeg_options.get("tune"):
        command.extend(["-tune", ffmpeg_options["tune"]])
    if ffmpeg_options.get("audio_codec"):
        command.extend(["-c:a", ffmpeg_options["audio_codec"]])
    if ffmpeg_options.get("audio_bitrate"):
        command.extend(["-b:a", ffmpeg_options["audio_bitrate"]])
    if ffmpeg_options.get("fps_mode"):
         command.extend(["-fps_mode", ffmpeg_options["fps_mode"]])
    # Add pix_fmt for broad compatibility
    command.extend(["-pix_fmt", "yuv420p"])
    if "other_flags" in ffmpeg_options:
        command.extend(ffmpeg_options["other_flags"])
    command.append(output_path)
    logger.info(f"Generated FFmpeg command: {' '.join(command)}")
    return command

def process_video_assembly(data, job_id):
    inputs = data.get("inputs", {})
    outputs_spec = data.get("outputs", [])
    if not inputs or not outputs_spec:
        raise ValueError("Invalid payload: Missing 'inputs' or 'outputs'.")
    output_filename = inputs.get("output_filename", f"video_assembly_{job_id}.mp4")
    audio_input = inputs.get("audio_input")
    image_sequence = inputs.get("image_sequence")
    ffmpeg_options = inputs.get("ffmpeg_options", {})
    output_target = outputs_spec[0]
    output_type = output_target.get("type", "gdrive") # Default to gdrive if not specified? Or error?

    if not audio_input or not image_sequence:
        raise ValueError("Invalid payload: Missing audio_input or image_sequence.")

    # --- Get Default Bucket Names from Config ---
    default_s3_bucket = getattr(config, 'S3_BUCKET_NAME', None)
    # Add default GDrive folder if needed, though it's in payload now
    # default_gdrive_folder = getattr(config, 'GDRIVE_OUTPUT_FOLDER', None)

    temp_dir = tempfile.mkdtemp(prefix=f"nca_{job_id}_", dir=config.LOCAL_STORAGE_PATH)
    logger.info(f"Job {job_id}: Created temporary directory: {temp_dir}")
    try:
        # --- 1. Download Files ---
        local_audio_path = None
        image_inputs_for_ffmpeg = []
        download_tasks = []

        # Prepare audio download task
        audio_source_type = audio_input.get("source_type")
        if audio_source_type == "gdrive_id" and audio_input.get("file_id"):
            audio_id = audio_input['file_id']
            ext = os.path.splitext(audio_input.get("file_name", "audio.mp3"))[1] or ".mp3"
            local_audio_path = os.path.join(temp_dir, f"audio_{audio_id}{ext}")
            download_tasks.append({"type": "gdrive", "id": audio_id, "path": local_audio_path})
        elif audio_source_type == "s3_object_key" and audio_input.get("object_key"):
            obj_key = audio_input['object_key']
            bucket = audio_input.get("bucket_name", default_s3_bucket)
            ext = os.path.splitext(audio_input.get("file_name", os.path.basename(obj_key)))[1] or ".tmp"
            local_audio_path = os.path.join(temp_dir, f"audio{ext}")
            download_tasks.append({"type": "s3", "bucket": bucket, "key": obj_key, "path": local_audio_path})
        else:
            raise ValueError("Invalid audio_input: Requires source_type 'gdrive_id' with 'file_id' OR 's3_object_key' with 'object_key'.")

        # Prepare image download tasks
        for i, img_data in enumerate(image_sequence):
            img_source_type = img_data.get("source_type")
            duration = img_data.get("duration")
            if duration is None: raise ValueError(f"Missing duration for image sequence item {i}.")

            local_img_path = None
            img_task = None
            ext = ".jpg" # Default extension
            original_name = img_data.get("file_name", f"image{i}")
            if '.' in original_name:
                 name_part, ext_part = os.path.splitext(original_name)
                 if ext_part: ext = ext_part

            if img_source_type == "gdrive_id" and img_data.get("file_id"):
                img_id = img_data['file_id']
                local_img_path = os.path.join(temp_dir, f"image_{i}_{img_id}{ext}")
                img_task = {"type": "gdrive", "id": img_id, "path": local_img_path}
            elif img_source_type == "s3_object_key" and img_data.get("object_key"):
                obj_key = img_data['object_key']
                bucket = img_data.get("bucket_name", default_s3_bucket)
                local_img_path = os.path.join(temp_dir, f"image_{i}{ext}")
                img_task = {"type": "s3", "bucket": bucket, "key": obj_key, "path": local_img_path}
            else:
                 raise ValueError(f"Invalid image_sequence item {i}: Requires source_type and file_id/object_key.")

            download_tasks.append(img_task)
            image_inputs_for_ffmpeg.append({'path': local_img_path, 'duration': duration}) # Store local path for ffmpeg

        # Execute downloads
        logger.info(f"Job {job_id}: Starting downloads for {len(download_tasks)} files...")
        for task in download_tasks:
            logger.info(f"Job {job_id}: Downloading {task['type']} source: {task.get('id') or task.get('key')}")
            if task['type'] == 'gdrive':
                gdrive_service.download_gdrive_file(task['id'], task['path'])
            elif task['type'] == 's3':
                if not task.get('bucket'): raise ValueError("Missing bucket name for S3 download.")
                s3_toolkit.download_file_from_s3(task['bucket'], task['key'], task['path'])
            logger.info(f"Job {job_id}: Downloaded to {task['path']}")
        logger.info(f"Job {job_id}: All downloads complete.")


        # --- 2. Generate and Run FFmpeg Command ---
        logger.info(f"Job {job_id}: Generating FFmpeg command...")
        local_output_path = os.path.join(temp_dir, output_filename)
        ffmpeg_command = generate_ffmpeg_command(
            image_inputs_for_ffmpeg,
            local_audio_path, # Use the downloaded local path
            local_output_path,
            ffmpeg_options
        )
        logger.info(f"Job {job_id}: Executing FFmpeg command...")
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logger.error(f"Job {job_id}: FFmpeg command failed. Stderr:\n{result.stderr}")
            raise Exception(f"FFmpeg execution failed: {result.stderr}")
        else:
             logger.info(f"Job {job_id}: FFmpeg command successful.") # Stdout/Stderr can be very long
             logger.info(f"Job {job_id}: Output video created at {local_output_path}")


        # --- 3. Upload Result ---
        final_output_details = {}
        if output_type == "gdrive":
            gdrive_output_folder_id = output_target.get("folder_id")
            if not gdrive_output_folder_id: raise ValueError("Missing folder_id for gdrive output.")
            logger.info(f"Job {job_id}: Uploading result to Google Drive folder {gdrive_output_folder_id}...")
            uploaded_file_info = gdrive_service.upload_file_to_gdrive(
                local_output_path, gdrive_output_folder_id, output_filename
            )
            logger.info(f"Job {job_id}: GDrive upload successful. File ID: {uploaded_file_info.get('id')}")
            final_output_details = {
                 "gdrive_id": uploaded_file_info.get('id'),
                 "filename": uploaded_file_info.get('name'),
                 "url": uploaded_file_info.get('webViewLink'),
                 "storage_type": "gdrive"
            }
        elif output_type == "s3":
            output_bucket = output_target.get("bucket_name", default_s3_bucket)
            output_key_prefix = output_target.get("object_key_prefix", f"youtube/{job_id}/") # Default to job-specific folder
            output_key = os.path.join(output_key_prefix, output_filename).replace("\\", "/") # Ensure forward slashes
            if not output_bucket: raise ValueError("Missing bucket_name for S3 output.")
            logger.info(f"Job {job_id}: Uploading result to S3 bucket {output_bucket} as {output_key}...")
            uploaded_file_info = s3_toolkit.upload_file_to_s3(
                local_output_path, output_bucket, output_key
            )
            logger.info(f"Job {job_id}: S3 upload successful.")
            final_output_details = {
                 "s3_bucket": uploaded_file_info.get('bucket'),
                 "s3_key": uploaded_file_info.get('object_key'),
                 "url": uploaded_file_info.get('url'),
                 "storage_type": "s3"
            }
        else:
            logger.error(f"Job {job_id}: Unsupported output type specified: {output_type}")
            raise ValueError(f"Unsupported output type: {output_type}")

        return final_output_details

    except Exception as e:
        logger.error(f"Job {job_id}: Error during video assembly process: {e}", exc_info=True)
        raise # Re-raise to be caught by the route handler/queue processor

    finally:
        # --- 4. Cleanup Temporary Files ---
        logger.info(f"Job {job_id}: Cleaning up temporary directory: {temp_dir}")
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Job {job_id}: Temporary directory cleaned up successfully.")
            except Exception as cleanup_error:
                logger.error(f"Job {job_id}: Error cleaning up temporary directory {temp_dir}: {cleanup_error}", exc_info=True)
