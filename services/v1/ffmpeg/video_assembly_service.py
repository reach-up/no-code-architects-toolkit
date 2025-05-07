# Copyright (c) 2025 Stephen G. Pope, modifications by [Your Name/AI]
# Based on NCA Toolkit structure
# Licensed under GPL-2.0

import os
import subprocess
import logging
import tempfile
import shutil
from services import gdrive_service
from services import file_management
import config

# Setup logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def generate_ffmpeg_command(image_inputs, audio_path, output_path, ffmpeg_options):
    command = ["ffmpeg"]
    filter_complex_parts = []
    input_mappings = []

    for i, img_input in enumerate(image_inputs):
        command.extend(["-loop", "1", "-t", str(img_input['duration']), "-i", img_input['path']])
        filter_complex_parts.append(f"[{i}:v]settb=AVTB,setpts=PTS-STARTPTS,fps=fps=25[v{i}];")
        input_mappings.append(f"[v{i}]")

    command.extend(["-i", audio_path])
    audio_input_index = len(image_inputs)

    filter_complex = "".join(filter_complex_parts)
    filter_complex += "".join(input_mappings)
    filter_complex += f"concat=n={len(image_inputs)}:v=1:a=0,format=pix_fmts=yuv420p[outv]"

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
    gdrive_output_folder_id = output_target.get("folder_id")

    if not audio_input or not image_sequence or not gdrive_output_folder_id:
        raise ValueError("Invalid payload: Missing audio_input, image_sequence, or output folder_id.")
    if audio_input.get("source_type") != "gdrive_id" or not audio_input.get("file_id"):
         raise ValueError("Invalid audio_input: source_type must be 'gdrive_id' with a valid 'file_id'.")
    if not all(img.get("source_type") == "gdrive_id" and img.get("file_id") and img.get("duration") for img in image_sequence):
         raise ValueError("Invalid image_sequence: All items must have source_type 'gdrive_id', file_id, and duration.")

    temp_dir = tempfile.mkdtemp(prefix=f"nca_{job_id}_", dir=config.LOCAL_STORAGE_PATH)
    logger.info(f"Job {job_id}: Created temporary directory: {temp_dir}")

    downloaded_files = {"audio": None, "images": []}
    try:
        logger.info(f"Job {job_id}: Downloading audio file...")
        audio_file_id = audio_input['file_id']
        local_audio_path = os.path.join(temp_dir, f"audio_{audio_file_id}")
        gdrive_service.download_gdrive_file(audio_file_id, local_audio_path)
        downloaded_files["audio"] = local_audio_path
        logger.info(f"Job {job_id}: Audio downloaded to {local_audio_path}")

        logger.info(f"Job {job_id}: Downloading image sequence...")
        image_inputs_for_ffmpeg = []
        for i, img_data in enumerate(image_sequence):
            img_file_id = img_data['file_id']
            file_ext = ".jpg"
            original_name = img_data.get("file_name", f"image{i}")
            if '.' in original_name:
                 file_ext = os.path.splitext(original_name)[1]

            local_img_path = os.path.join(temp_dir, f"image_{i}_{img_file_id}{file_ext}")
            gdrive_service.download_gdrive_file(img_file_id, local_img_path)
            downloaded_files["images"].append(local_img_path)
            image_inputs_for_ffmpeg.append({'path': local_img_path, 'duration': img_data['duration']})
            logger.info(f"Job {job_id}: Image {i} downloaded to {local_img_path}")

        logger.info(f"Job {job_id}: Generating FFmpeg command...")
        local_output_path = os.path.join(temp_dir, output_filename)
        ffmpeg_command = generate_ffmpeg_command(
            image_inputs_for_ffmpeg,
            local_audio_path,
            local_output_path,
            ffmpeg_options
        )

        logger.info(f"Job {job_id}: Executing FFmpeg command...")
        result = subprocess.run(ffmpeg_command, capture_output=True, text=True, check=False) # Use check=False to handle errors manually

        if result.returncode != 0:
            logger.error(f"Job {job_id}: FFmpeg command failed. Stderr:\n{result.stderr}")
            raise Exception(f"FFmpeg execution failed: {result.stderr}")
        else:
             logger.info(f"Job {job_id}: FFmpeg command successful. Stdout:\n{result.stdout}\nStderr:\n{result.stderr}")
             logger.info(f"Job {job_id}: Output video created at {local_output_path}")

        logger.info(f"Job {job_id}: Uploading result to Google Drive folder {gdrive_output_folder_id}...")
        uploaded_file_info = gdrive_service.upload_file_to_gdrive(
            local_output_path,
            gdrive_output_folder_id,
            output_filename
        )
        logger.info(f"Job {job_id}: Upload successful. File ID: {uploaded_file_info.get('id')}")

        final_output_details = {
             "gdrive_id": uploaded_file_info.get('id'),
             "filename": uploaded_file_info.get('name'),
             "url": uploaded_file_info.get('webViewLink'),
             "storage_type": "gdrive"
        }
        return final_output_details

    except Exception as e:
        logger.error(f"Job {job_id}: Error during video assembly process: {e}", exc_info=True)
        raise

    finally:
        logger.info(f"Job {job_id}: Cleaning up temporary directory: {temp_dir}")
        if os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
                logger.info(f"Job {job_id}: Temporary directory cleaned up successfully.")
            except Exception as cleanup_error:
                logger.error(f"Job {job_id}: Error cleaning up temporary directory {temp_dir}: {cleanup_error}", exc_info=True)
