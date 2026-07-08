"""
test_workers_intensive.py

Real-workload tests for broccoli workers. Every task does genuine work:
actual video transcoding with ffmpeg, image processing with OpenCV/PIL,
audio synthesis with scipy, signal analysis, SQLite indexing, ML inference,
and image compression pipelines. No mocking, no sleep-based simulation.

Requirements: ffmpeg, imagemagick, numpy, scipy, cv2 (opencv-python), Pillow, sklearn
"""

import hashlib
import io
import json
import math
import os
import sqlite3
import struct
import subprocess
import tempfile
import time
import uuid
import wave
import zipfile
from pathlib import Path

import cv2
import numpy as np
from PIL import Image, ImageDraw, ImageFilter, ImageFont
from scipy import fft, ndimage, signal
from scipy.io import wavfile
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler

from broccoli.core.chain.task_chain import TaskChain
from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.core.task.task_registry import TaskRegistry
from broccoli.workers.chain_worker import ChainWorker
from broccoli.workers.threaded_worker import ThreadedWorker
from broccoli.workers.worker_pool import WorkerPool

registry = TaskRegistry()
queue = TaskQueue()
chain = TaskChain()

WORKDIR = Path(tempfile.mkdtemp(prefix="broccoli_test_"))
print(f"Working directory: {WORKDIR}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def workdir(name: str) -> str:
    return str(WORKDIR / name)


# ---------------------------------------------------------------------------
# FIXTURE GENERATORS — build real source files before tests run
# ---------------------------------------------------------------------------


def generate_real_video(
    path: str, duration_sec: int = 5, width: int = 640, height: int = 360
) -> str:
    """
    Generate a real H.264 MP4 using ffmpeg's built-in test sources
    (testsrc + sine tone). No dummy bytes — it's a decodable video.
    """
    cmd = [
        "ffmpeg",
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"testsrc=duration={duration_sec}:size={width}x{height}:rate=24",
        "-f",
        "lavfi",
        "-i",
        f"sine=frequency=440:duration={duration_sec}",
        "-c:v",
        "libx264",
        "-preset",
        "ultrafast",
        "-crf",
        "28",
        "-c:a",
        "aac",
        "-b:a",
        "64k",
        "-movflags",
        "+faststart",
        path,
    ]
    subprocess.run(cmd, check=True, capture_output=True)
    return path


def generate_real_audio_wav(
    path: str, duration_sec: float = 3.0, sample_rate: int = 44100
) -> str:
    """Generate a real multi-tone WAV file using scipy."""
    t = np.linspace(0, duration_sec, int(sample_rate * duration_sec), endpoint=False)
    # Mix of 440 Hz + 880 Hz + 1320 Hz with slight frequency modulation
    wave_data = (
        0.4 * np.sin(2 * np.pi * 440 * t)
        + 0.3 * np.sin(2 * np.pi * 880 * t + 0.5 * np.sin(2 * np.pi * 2 * t))
        + 0.3 * np.sin(2 * np.pi * 1320 * t)
    )
    wave_data = (wave_data * 32767).astype(np.int16)
    wavfile.write(path, sample_rate, wave_data)
    return path


def generate_real_image(path: str, width: int = 1024, height: int = 768) -> str:
    """Generate a real image with gradients, shapes and noise — not blank."""
    img = Image.new("RGB", (width, height))
    draw = ImageDraw.Draw(img)

    # Gradient background
    for y in range(height):
        r = int(255 * y / height)
        g = int(128 + 127 * math.sin(y / 50))
        b = int(255 * (1 - y / height))
        draw.line([(0, y), (width, y)], fill=(r, g, b))

    # Some geometric shapes
    for i in range(20):
        x0, y0 = np.random.randint(0, width - 100), np.random.randint(0, height - 100)
        x1, y1 = x0 + np.random.randint(30, 150), y0 + np.random.randint(30, 150)
        color = tuple(np.random.randint(0, 255, 3).tolist())
        draw.ellipse([x0, y0, x1, y1], outline=color, width=3)
        draw.rectangle([x0 + 10, y0 + 10, x1 - 10, y1 - 10], outline=color, width=2)

    # Add Gaussian noise via numpy
    arr = np.array(img).astype(np.float32)
    noise = np.random.normal(0, 15, arr.shape)
    arr = np.clip(arr + noise, 0, 255).astype(np.uint8)
    Image.fromarray(arr).save(path, quality=95)
    return path


def generate_sqlite_db(path: str, num_rows: int = 50_000) -> str:
    """Create a real SQLite database with data that needs indexing/aggregation."""
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE events (
            id INTEGER PRIMARY KEY,
            timestamp REAL,
            user_id INTEGER,
            event_type TEXT,
            duration_ms INTEGER,
            payload TEXT
        )
    """)
    rng = np.random.default_rng(42)
    event_types = ["click", "view", "purchase", "scroll", "search", "logout"]
    rows = [
        (
            float(time.time()) - float(rng.integers(0, 30 * 86400)),
            int(rng.integers(1, 1000)),
            event_types[int(rng.integers(0, len(event_types)))],
            int(rng.integers(1, 5000)),
            json.dumps(
                {"value": float(rng.random()), "session": str(uuid.uuid4())[:8]}
            ),
        )
        for _ in range(num_rows)
    ]
    c.executemany(
        "INSERT INTO events (timestamp, user_id, event_type, duration_ms, payload) VALUES (?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()
    return path


# ---------------------------------------------------------------------------
# TASK DEFINITIONS — registered with the global registry
# ---------------------------------------------------------------------------


# ── Video ──────────────────────────────────────────────────────────────────


@registry.register("transcode_h264_to_hevc")
def transcode_h264_to_hevc(payload):
    """
    Real transcode: H.264 → H.265/HEVC via ffmpeg.
    Reads every frame, re-encodes with libx265.
    """
    src = payload["input_path"]
    dst = payload["output_path"]
    crf = payload.get("crf", 28)
    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        src,
        "-c:v",
        "libx265",
        "-preset",
        "fast",
        f"-crf",
        str(crf),
        "-c:a",
        "copy",
        dst,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg transcode failed:\n{result.stderr}")

    probe_cmd = [
        "ffprobe",
        "-v",
        "quiet",
        "-print_format",
        "json",
        "-show_streams",
        "-show_format",
        dst,
    ]
    probe = subprocess.run(probe_cmd, capture_output=True, text=True)
    info = json.loads(probe.stdout)
    fmt = info.get("format", {})
    return {
        "input_size_mb": os.path.getsize(src) / 1e6,
        "output_size_mb": os.path.getsize(dst) / 1e6,
        "duration_sec": float(fmt.get("duration", 0)),
        "bit_rate_kbps": int(fmt.get("bit_rate", 0)) // 1000,
        "codec": "hevc",
    }


@registry.register("extract_video_frames")
def extract_video_frames(payload):
    """
    Extract every Nth frame from a video as PNG using ffmpeg.
    Returns frame count, resolution, and a perceptual hash of the first frame.
    """
    src = payload["input_path"]
    out_dir = payload["output_dir"]
    every_n = payload.get("every_n_frames", 24)
    Path(out_dir).mkdir(parents=True, exist_ok=True)

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        src,
        "-vf",
        f"select='not(mod(n\\,{every_n}))'",
        "-vsync",
        "vfr",
        f"{out_dir}/frame_%04d.png",
    ]
    subprocess.run(cmd, check=True, capture_output=True)

    frames = sorted(Path(out_dir).glob("frame_*.png"))
    if not frames:
        raise RuntimeError("No frames extracted")

    # Compute perceptual hash of first frame
    img = cv2.imread(str(frames[0]), cv2.IMREAD_GRAYSCALE)
    small = cv2.resize(img, (8, 8))
    avg = small.mean()
    phash = "".join("1" if p > avg else "0" for p in small.flatten())

    return {
        "frames_extracted": len(frames),
        "first_frame_path": str(frames[0]),
        "frame_phash": phash,
        "resolution": f"{img.shape[1]}x{img.shape[0]}",
    }


@registry.register("generate_video_thumbnail")
def generate_video_thumbnail(payload):
    """
    Grab a frame at a specific timestamp and produce a thumbnail with ffmpeg,
    then apply PIL sharpening and border overlay.
    """
    src = payload["input_path"]
    dst = payload["output_path"]
    timestamp = payload.get("timestamp_sec", 1.0)
    width = payload.get("width", 320)

    cmd = [
        "ffmpeg",
        "-y",
        "-ss",
        str(timestamp),
        "-i",
        src,
        "-vframes",
        "1",
        "-vf",
        f"scale={width}:-1",
        dst,
    ]
    subprocess.run(cmd, check=True, capture_output=True)

    # PIL post-processing: sharpen + white border
    img = Image.open(dst)
    img = img.filter(ImageFilter.SHARPEN)
    bordered = Image.new("RGB", (img.width + 10, img.height + 10), (255, 255, 255))
    bordered.paste(img, (5, 5))
    bordered.save(dst)

    return {
        "thumbnail_path": dst,
        "width": bordered.width,
        "height": bordered.height,
        "size_kb": os.path.getsize(dst) / 1024,
    }


@registry.register("extract_audio_from_video")
def extract_audio_from_video(payload):
    """
    Demux and re-encode audio from video to WAV using ffmpeg.
    """
    src = payload["input_path"]
    dst = payload["output_path"]
    sample_rate = payload.get("sample_rate", 44100)

    cmd = [
        "ffmpeg",
        "-y",
        "-i",
        src,
        "-vn",
        "-acodec",
        "pcm_s16le",
        "-ar",
        str(sample_rate),
        "-ac",
        "1",
        dst,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Audio extraction failed:\n{result.stderr}")

    sr, data = wavfile.read(dst)
    duration = len(data) / sr
    rms = float(np.sqrt(np.mean(data.astype(np.float64) ** 2)))
    return {
        "output_path": dst,
        "sample_rate": sr,
        "duration_sec": round(duration, 3),
        "rms_amplitude": round(rms, 2),
        "num_samples": len(data),
    }


# ── Audio / Signal Processing ──────────────────────────────────────────────


@registry.register("audio_fft_analysis")
def audio_fft_analysis(payload):
    """
    Load a WAV file, compute a full FFT, find dominant frequencies,
    and compute spectral centroid. Real scipy signal processing.
    """
    path = payload["wav_path"]
    top_n = payload.get("top_n_frequencies", 10)

    sr, data = wavfile.read(path)
    if data.ndim > 1:
        data = data.mean(axis=1)
    data = data.astype(np.float64)

    # Full FFT
    spectrum = np.abs(fft.rfft(data))
    freqs = fft.rfftfreq(len(data), d=1.0 / sr)

    # Top N dominant frequencies
    top_idx = np.argsort(spectrum)[-top_n:][::-1]
    dominant = [
        {
            "freq_hz": round(float(freqs[i]), 2),
            "magnitude": round(float(spectrum[i]), 2),
        }
        for i in top_idx
    ]

    # Spectral centroid
    centroid = float(np.sum(freqs * spectrum) / np.sum(spectrum))

    # Spectral rolloff (95% of energy)
    cumulative = np.cumsum(spectrum)
    rolloff_idx = np.searchsorted(cumulative, 0.95 * cumulative[-1])
    rolloff_freq = float(freqs[min(rolloff_idx, len(freqs) - 1)])

    return {
        "sample_rate": sr,
        "num_samples": len(data),
        "duration_sec": round(len(data) / sr, 3),
        "dominant_frequencies": dominant,
        "spectral_centroid_hz": round(centroid, 2),
        "spectral_rolloff_95pct_hz": round(rolloff_freq, 2),
    }


@registry.register("audio_bandpass_filter")
def audio_bandpass_filter(payload):
    """
    Apply a real Butterworth bandpass filter to a WAV file and write the result.
    """
    src = payload["wav_path"]
    dst = payload["output_path"]
    low_hz = payload.get("low_hz", 300)
    high_hz = payload.get("high_hz", 3400)
    order = payload.get("order", 5)

    sr, data = wavfile.read(src)
    if data.ndim > 1:
        data = data.mean(axis=1).astype(np.int16)

    nyq = sr / 2.0
    sos = signal.butter(
        order, [low_hz / nyq, high_hz / nyq], btype="band", output="sos"
    )
    filtered = signal.sosfilt(sos, data.astype(np.float64))
    filtered_int16 = np.clip(filtered, -32768, 32767).astype(np.int16)
    wavfile.write(dst, sr, filtered_int16)

    # Verify output energy difference
    original_rms = float(np.sqrt(np.mean(data.astype(np.float64) ** 2)))
    filtered_rms = float(np.sqrt(np.mean(filtered.astype(np.float64) ** 2)))

    return {
        "output_path": dst,
        "original_rms": round(original_rms, 2),
        "filtered_rms": round(filtered_rms, 2),
        "energy_ratio": round(filtered_rms / original_rms if original_rms else 0, 4),
        "filter": f"butterworth bandpass {low_hz}-{high_hz} Hz order={order}",
    }


@registry.register("generate_spectrogram")
def generate_spectrogram(payload):
    """
    Compute a Short-Time Fourier Transform spectrogram and save it as a PNG image.
    """
    src = payload["wav_path"]
    dst = payload["output_path"]
    fft_size = payload.get("fft_size", 1024)
    hop = payload.get("hop", 512)

    sr, data = wavfile.read(src)
    if data.ndim > 1:
        data = data.mean(axis=1)
    data = data.astype(np.float64) / 32768.0

    f, t_frames, Zxx = signal.stft(
        data, fs=sr, nperseg=fft_size, noverlap=fft_size - hop
    )
    magnitude_db = 20 * np.log10(np.abs(Zxx) + 1e-10)

    # Normalize to 0-255 image
    mag_norm = magnitude_db - magnitude_db.min()
    if mag_norm.max() > 0:
        mag_norm = mag_norm / mag_norm.max()
    img_arr = (mag_norm * 255).astype(np.uint8)
    img_arr = np.flipud(img_arr)  # Low freq at bottom

    # False-color (hot colormap approximation)
    h, w = img_arr.shape
    rgb = np.zeros((h, w, 3), dtype=np.uint8)
    rgb[:, :, 0] = np.clip(img_arr * 2, 0, 255)
    rgb[:, :, 1] = np.clip((img_arr.astype(int) - 128) * 2, 0, 255).astype(np.uint8)
    rgb[:, :, 2] = np.clip(255 - img_arr * 2, 0, 255)

    Image.fromarray(rgb).save(dst)

    return {
        "output_path": dst,
        "spectrogram_shape": list(img_arr.shape),
        "time_bins": len(t_frames),
        "freq_bins": len(f),
        "max_freq_hz": round(float(f[-1]), 1),
    }


# ── Image Processing ───────────────────────────────────────────────────────


@registry.register("image_edge_detect_and_contours")
def image_edge_detect_and_contours(payload):
    """
    Real OpenCV pipeline: Gaussian blur → Canny edges → contour detection →
    draw bounding boxes → save annotated image.
    """
    src = payload["input_path"]
    dst = payload["output_path"]

    img = cv2.imread(src)
    if img is None:
        raise ValueError(f"Could not open image: {src}")

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
    blurred = cv2.GaussianBlur(gray, (5, 5), 0)
    edges = cv2.Canny(blurred, threshold1=50, threshold2=150)

    contours, hierarchy = cv2.findContours(
        edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE
    )

    # Filter and draw bounding boxes for significant contours
    significant = [c for c in contours if cv2.contourArea(c) > 100]
    annotated = img.copy()
    for c in significant:
        x, y, w, h = cv2.boundingRect(c)
        cv2.rectangle(annotated, (x, y), (x + w, y + h), (0, 255, 0), 2)

    cv2.imwrite(dst, annotated)

    return {
        "output_path": dst,
        "total_contours": len(contours),
        "significant_contours": len(significant),
        "image_resolution": f"{img.shape[1]}x{img.shape[0]}",
        "edge_pixels": int(np.count_nonzero(edges)),
    }


@registry.register("image_color_cluster")
def image_color_cluster(payload):
    """
    Extract the dominant color palette via K-Means clustering on image pixels.
    Real sklearn KMeans on real pixel data.
    """
    src = payload["input_path"]
    dst = payload["output_path"]
    n_colors = payload.get("n_colors", 8)
    max_pixels = payload.get("max_pixels", 10_000)

    img = cv2.imread(src)
    img_rgb = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    pixels = img_rgb.reshape(-1, 3).astype(np.float32)

    # Subsample for speed
    if len(pixels) > max_pixels:
        idx = np.random.choice(len(pixels), max_pixels, replace=False)
        pixels = pixels[idx]

    kmeans = KMeans(n_clusters=n_colors, n_init=10, random_state=42)
    kmeans.fit(pixels)
    centers = kmeans.cluster_centers_.astype(int)
    counts = np.bincount(kmeans.labels_)
    total = counts.sum()

    palette = [
        {
            "r": int(c[0]),
            "g": int(c[1]),
            "b": int(c[2]),
            "hex": f"#{c[0]:02x}{c[1]:02x}{c[2]:02x}",
            "proportion": round(float(counts[i]) / total, 4),
        }
        for i, c in enumerate(centers)
    ]
    palette.sort(key=lambda x: x["proportion"], reverse=True)

    # Build a palette swatch image
    swatch_w, swatch_h = 80, 80
    swatch_img = Image.new("RGB", (swatch_w * n_colors, swatch_h))
    d = ImageDraw.Draw(swatch_img)
    for i, p in enumerate(palette):
        d.rectangle(
            [i * swatch_w, 0, (i + 1) * swatch_w, swatch_h],
            fill=(p["r"], p["g"], p["b"]),
        )
    swatch_img.save(dst)

    return {
        "swatch_path": dst,
        "n_colors": n_colors,
        "palette": palette,
        "pixels_sampled": len(pixels),
    }


@registry.register("image_pca_compress")
def image_pca_compress(payload):
    """
    Compress an image using PCA (truncated SVD per channel) and reconstruct it.
    Illustrates information loss vs component count.
    """
    src = payload["input_path"]
    dst = payload["output_path"]
    n_components = payload.get("n_components", 50)

    img = Image.open(src).convert("RGB")
    arr = np.array(img).astype(np.float32)

    reconstructed_channels = []
    explained = []
    for ch in range(3):
        channel = arr[:, :, ch]
        scaler = StandardScaler()
        scaled = scaler.fit_transform(channel)
        pca = PCA(n_components=min(n_components, min(channel.shape) - 1))
        compressed = pca.fit_transform(scaled)
        reconstructed = pca.inverse_transform(compressed)
        reconstructed = scaler.inverse_transform(reconstructed)
        reconstructed_channels.append(reconstructed)
        explained.append(float(pca.explained_variance_ratio_.sum()))

    result_arr = np.stack(reconstructed_channels, axis=2)
    result_arr = np.clip(result_arr, 0, 255).astype(np.uint8)
    Image.fromarray(result_arr).save(dst)

    return {
        "output_path": dst,
        "n_components": n_components,
        "variance_explained_per_channel": {
            "R": round(explained[0], 4),
            "G": round(explained[1], 4),
            "B": round(explained[2], 4),
        },
        "original_size_kb": round(os.path.getsize(src) / 1024, 2),
        "compressed_size_kb": round(os.path.getsize(dst) / 1024, 2),
    }


@registry.register("image_batch_resize_and_pack")
def image_batch_resize_and_pack(payload):
    """
    Resize a list of images to multiple resolutions and pack all into a ZIP.
    """
    input_paths = payload["input_paths"]
    output_zip = payload["output_zip"]
    sizes = payload.get("sizes", [(320, 240), (640, 480), (1280, 720)])

    buf = io.BytesIO()
    total_written = 0
    file_count = 0

    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for src_path in input_paths:
            img = Image.open(src_path).convert("RGB")
            stem = Path(src_path).stem
            for w, h in sizes:
                resized = img.resize((w, h), Image.LANCZOS)
                img_buf = io.BytesIO()
                resized.save(img_buf, format="JPEG", quality=85, optimize=True)
                img_bytes = img_buf.getvalue()
                zf.writestr(f"{stem}_{w}x{h}.jpg", img_bytes)
                total_written += len(img_bytes)
                file_count += 1

    Path(output_zip).write_bytes(buf.getvalue())
    return {
        "output_zip": output_zip,
        "zip_size_kb": round(os.path.getsize(output_zip) / 1024, 2),
        "files_in_zip": file_count,
        "total_image_bytes": total_written,
        "images_processed": len(input_paths),
        "sizes_per_image": len(sizes),
    }


# ── Database ───────────────────────────────────────────────────────────────


@registry.register("sqlite_build_indexes")
def sqlite_build_indexes(payload):
    """
    Open a real SQLite database and create multi-column indexes.
    Measures query speedup before/after.
    """
    db_path = payload["db_path"]
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Measure time without index
    t0 = time.perf_counter()
    c.execute(
        """
        SELECT event_type, COUNT(*) as cnt, AVG(duration_ms)
        FROM events
        WHERE timestamp > ?
        GROUP BY event_type
        ORDER BY cnt DESC
    """,
        (time.time() - 7 * 86400,),
    )
    rows_before = c.fetchall()
    time_before = time.perf_counter() - t0

    # Build indexes
    c.execute("CREATE INDEX IF NOT EXISTS idx_ts ON events(timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_type_ts ON events(event_type, timestamp)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_user ON events(user_id, event_type)")
    conn.commit()

    # Measure again
    t0 = time.perf_counter()
    c.execute(
        """
        SELECT event_type, COUNT(*) as cnt, AVG(duration_ms)
        FROM events
        WHERE timestamp > ?
        GROUP BY event_type
        ORDER BY cnt DESC
    """,
        (time.time() - 7 * 86400,),
    )
    rows_after = c.fetchall()
    time_after = time.perf_counter() - t0
    conn.close()

    speedup = round(time_before / time_after if time_after > 0 else 1.0, 2)
    return {
        "rows_in_result": len(rows_after),
        "query_time_before_ms": round(time_before * 1000, 2),
        "query_time_after_ms": round(time_after * 1000, 2),
        "speedup_factor": speedup,
        "indexes_created": ["idx_ts", "idx_type_ts", "idx_user"],
    }


@registry.register("sqlite_aggregate_report")
def sqlite_aggregate_report(payload):
    """
    Run complex multi-CTE analytical queries against a SQLite database.
    Computes user funnels, percentiles, and session statistics.
    """
    db_path = payload["db_path"]
    output_path = payload.get("output_path")
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Per-user event counts and average duration
    c.execute("""
        SELECT
            user_id,
            COUNT(*) as total_events,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            AVG(duration_ms) as avg_duration,
            MAX(duration_ms) as max_duration,
            MIN(timestamp) as first_seen,
            MAX(timestamp) as last_seen
        FROM events
        GROUP BY user_id
        ORDER BY total_events DESC
        LIMIT 100
    """)
    user_stats = c.fetchall()

    # Hourly event distribution
    c.execute("""
        SELECT
            CAST((timestamp % 86400) / 3600 AS INTEGER) as hour_of_day,
            event_type,
            COUNT(*) as cnt
        FROM events
        GROUP BY hour_of_day, event_type
        ORDER BY hour_of_day, cnt DESC
    """)
    hourly = c.fetchall()

    # Conversion funnel: view → click → purchase
    c.execute("SELECT COUNT(DISTINCT user_id) FROM events WHERE event_type = 'view'")
    viewers = c.fetchone()[0]
    c.execute("SELECT COUNT(DISTINCT user_id) FROM events WHERE event_type = 'click'")
    clickers = c.fetchone()[0]
    c.execute(
        "SELECT COUNT(DISTINCT user_id) FROM events WHERE event_type = 'purchase'"
    )
    buyers = c.fetchone()[0]

    conn.close()

    report = {
        "top_users_analyzed": len(user_stats),
        "hourly_buckets": len(hourly),
        "funnel": {
            "viewers": viewers,
            "clickers": clickers,
            "buyers": buyers,
            "view_to_click_pct": round(clickers / viewers * 100, 2) if viewers else 0,
            "click_to_buy_pct": round(buyers / clickers * 100, 2) if clickers else 0,
        },
    }

    if output_path:
        with open(output_path, "w") as f:
            json.dump(report, f, indent=2)

    return report


@registry.register("sqlite_full_text_search_setup")
def sqlite_full_text_search_setup(payload):
    """
    Build a real FTS5 virtual table from the events payload column,
    insert all rows, and run a scored text search.
    """
    db_path = payload["db_path"]
    search_term = payload.get("search_term", "session")

    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Build FTS5 table
    c.execute("DROP TABLE IF EXISTS events_fts")
    c.execute("""
        CREATE VIRTUAL TABLE events_fts USING fts5(
            event_id UNINDEXED,
            event_type,
            payload,
            content='events',
            content_rowid='id'
        )
    """)
    c.execute("""
        INSERT INTO events_fts(event_id, event_type, payload)
        SELECT id, event_type, payload FROM events
    """)
    conn.commit()

    # Full-text search
    c.execute(
        """
        SELECT event_id, event_type, payload, rank
        FROM events_fts
        WHERE events_fts MATCH ?
        ORDER BY rank
        LIMIT 20
    """,
        (search_term,),
    )
    results = c.fetchall()
    conn.close()

    return {
        "search_term": search_term,
        "results_found": len(results),
        "sample_result": results[0] if results else None,
    }


# ── Hashing / Integrity ────────────────────────────────────────────────────


@registry.register("multi_algorithm_hash")
def multi_algorithm_hash(payload):
    """
    Hash a file with MD5, SHA-1, SHA-256, SHA-512, and BLAKE2b simultaneously
    in a single streaming pass through the file.
    """
    path = payload["file_path"]
    chunk_size = payload.get("chunk_size", 1024 * 64)

    hashers = {
        "md5": hashlib.md5(),
        "sha1": hashlib.sha1(),
        "sha256": hashlib.sha256(),
        "sha512": hashlib.sha512(),
        "blake2b": hashlib.blake2b(),
    }
    total_bytes = 0

    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            for h in hashers.values():
                h.update(chunk)
            total_bytes += len(chunk)

    return {
        "file_path": path,
        "size_bytes": total_bytes,
        "hashes": {name: h.hexdigest() for name, h in hashers.items()},
    }


@registry.register("find_duplicate_files_by_hash")
def find_duplicate_files_by_hash(payload):
    """
    Find duplicate files by comparing SHA-256 hashes.
    Groups duplicates and computes total wasted space.
    """
    paths = payload["file_paths"]
    hashes = {}

    for path in paths:
        p = Path(path)
        if not p.exists():
            continue
        h = hashlib.sha256()
        with open(p, "rb") as f:
            while chunk := f.read(65536):
                h.update(chunk)
        digest = h.hexdigest()
        hashes.setdefault(digest, []).append(str(p))

    duplicates = {h: paths for h, paths in hashes.items() if len(paths) > 1}
    wasted_bytes = sum(
        os.path.getsize(paths[0]) * (len(paths) - 1) for paths in duplicates.values()
    )

    return {
        "files_scanned": len(paths),
        "unique_files": len(hashes),
        "duplicate_groups": len(duplicates),
        "wasted_space_kb": round(wasted_bytes / 1024, 2),
        "duplicates": duplicates,
    }


# ---------------------------------------------------------------------------
# CHAIN COMPLETION HANDLER
# ---------------------------------------------------------------------------


@registry.register("on_chain_finished")
def on_chain_finished(payload):
    chain_id = payload.get("chain_id")
    result = payload.get("result")
    print(
        f"\n✓ Chain {chain_id} finished. Final result: {json.dumps(result, indent=2)}"
    )
    return {"acknowledged": True, "chain_id": chain_id}


# ---------------------------------------------------------------------------
# TEST SCENARIOS
# ---------------------------------------------------------------------------


def test_video_transcode_pipeline():
    """
    Chain: transcode H.264→HEVC → extract frames → generate thumbnail → extract audio
    All real ffmpeg work. Tests ChainWorker and sequential data passing.
    """
    print("\n=== TEST 1: Video Transcode Pipeline (Chain) ===")

    src_video = workdir("source.mp4")
    hevc_video = workdir("transcoded.hevc.mp4")
    frames_dir = workdir("frames")
    thumbnail = workdir("thumbnail.jpg")
    audio_out = workdir("audio.wav")

    generate_real_video(src_video, duration_sec=6, width=640, height=360)
    print(f"Source video: {os.path.getsize(src_video) / 1e6:.2f} MB")

    worker = ChainWorker()
    chain_id = chain.chain(
        [
            {
                "task_type": "transcode_h264_to_hevc",
                "payload": {
                    "input_path": src_video,
                    "output_path": hevc_video,
                    "crf": 30,
                },
            },
            {
                "task_type": "extract_video_frames",
                "payload": {
                    "input_path": hevc_video,
                    "output_dir": frames_dir,
                    "every_n_frames": 24,
                },
            },
            {
                "task_type": "generate_video_thumbnail",
                "payload": {
                    "input_path": hevc_video,
                    "output_path": thumbnail,
                    "timestamp_sec": 1.5,
                    "width": 320,
                },
            },
            {
                "task_type": "extract_audio_from_video",
                "payload": {
                    "input_path": hevc_video,
                    "output_path": audio_out,
                    "sample_rate": 44100,
                },
            },
        ],
        completion_task="on_chain_finished",
    )
    print(f"Chain ID: {chain_id}")
    worker.start()

    status = chain.get_chain_status(chain_id)
    print(f"Chain status: {status}")


def test_audio_processing_chain():
    """
    Chain: FFT analysis → bandpass filter → spectrogram generation
    Real scipy signal processing on a generated WAV file.
    """
    print("\n=== TEST 2: Audio DSP Chain ===")

    src_wav = workdir("source_audio.wav")
    filtered_wav = workdir("filtered_audio.wav")
    spectrogram_img = workdir("spectrogram.png")

    generate_real_audio_wav(src_wav, duration_sec=4.0)
    print(f"Source WAV: {os.path.getsize(src_wav) / 1024:.1f} KB")

    worker = ChainWorker()
    chain_id = chain.chain(
        [
            {
                "task_type": "audio_fft_analysis",
                "payload": {"wav_path": src_wav, "top_n_frequencies": 15},
            },
            {
                "task_type": "audio_bandpass_filter",
                "payload": {
                    "wav_path": src_wav,
                    "output_path": filtered_wav,
                    "low_hz": 400,
                    "high_hz": 1500,
                    "order": 6,
                },
            },
            {
                "task_type": "generate_spectrogram",
                "payload": {
                    "wav_path": filtered_wav,
                    "output_path": spectrogram_img,
                    "fft_size": 2048,
                    "hop": 512,
                },
            },
        ],
        completion_task="on_chain_finished",
    )
    print(f"Chain ID: {chain_id}")
    worker.start()


def test_image_ml_pipeline():
    """
    Chain: edge detection → color clustering (KMeans) → PCA compression → batch resize+pack
    Real OpenCV + sklearn + PIL work.
    """
    print("\n=== TEST 3: Image ML Pipeline (Chain) ===")

    images = []
    for i in range(4):
        p = workdir(f"image_{i}.jpg")
        generate_real_image(p, width=1024, height=768)
        images.append(p)
    print(f"Generated {len(images)} real images")

    edge_out = workdir("edges_annotated.jpg")
    palette_out = workdir("palette_swatch.png")
    pca_out = workdir("pca_reconstructed.jpg")
    zip_out = workdir("all_sizes.zip")

    worker = ChainWorker()
    chain_id = chain.chain(
        [
            {
                "task_type": "image_edge_detect_and_contours",
                "payload": {"input_path": images[0], "output_path": edge_out},
            },
            {
                "task_type": "image_color_cluster",
                "payload": {
                    "input_path": images[0],
                    "output_path": palette_out,
                    "n_colors": 10,
                    "max_pixels": 20_000,
                },
            },
            {
                "task_type": "image_pca_compress",
                "payload": {
                    "input_path": images[0],
                    "output_path": pca_out,
                    "n_components": 40,
                },
            },
            {
                "task_type": "image_batch_resize_and_pack",
                "payload": {
                    "input_paths": images,
                    "output_zip": zip_out,
                    "sizes": [[320, 240], [640, 480], [1280, 720]],
                },
            },
        ],
        completion_task="on_chain_finished",
    )
    print(f"Chain ID: {chain_id}")
    worker.start()


def test_database_pipeline():
    """
    Chain: build indexes → run aggregate report → set up FTS
    Real SQLite with 50k rows, real query plans, real FTS5.
    """
    print("\n=== TEST 4: Database Analytics Pipeline (Chain) ===")

    db_path = workdir("events.db")
    report_path = workdir("report.json")

    print("Building SQLite database (50k rows)...")
    generate_sqlite_db(db_path, num_rows=50_000)
    print(f"DB size: {os.path.getsize(db_path) / 1e6:.2f} MB")

    worker = ChainWorker()
    chain_id = chain.chain(
        [
            {"task_type": "sqlite_build_indexes", "payload": {"db_path": db_path}},
            {
                "task_type": "sqlite_aggregate_report",
                "payload": {"db_path": db_path, "output_path": report_path},
            },
            {
                "task_type": "sqlite_full_text_search_setup",
                "payload": {"db_path": db_path, "search_term": "purchase"},
            },
        ],
        completion_task="on_chain_finished",
    )
    print(f"Chain ID: {chain_id}")
    worker.start()


def test_file_integrity_pool():
    """
    WorkerPool stress test: hash every file with 5 algorithms in parallel.
    Exercises the pool under concurrent CPU load.
    """
    print("\n=== TEST 5: File Integrity Pool (WorkerPool) ===")

    files = []
    for i in range(8):
        p = workdir(f"hashme_{i}.bin")
        # Write random binary data of varying sizes (500KB–4MB)
        size = (i + 1) * 500 * 1024
        Path(p).write_bytes(os.urandom(size))
        files.append(p)

    print(f"Generated {len(files)} binary files for hashing")

    for f in files:
        task = Task(task_type="multi_algorithm_hash", payload={"file_path": f})
        queue.push(task)

    pool = WorkerPool(
        worker_type=ThreadedWorker, num_workers=4, redis_url="redis://localhost:6379"
    )
    print("Starting pool of 4 workers...")
    pool.start()


def test_duplicate_detection_and_audio_analysis():
    """
    Parallel independent tasks via ThreadedWorker:
    - Duplicate file detection across many files
    - Multi-algorithm hashing
    - FFT analysis on multiple audio files
    Tests concurrent CPU-bound tasks.
    """
    print("\n=== TEST 6: Parallel CPU Tasks (ThreadedWorker) ===")

    # Create a mix of unique and duplicate files
    file_paths = []
    for i in range(6):
        p = workdir(f"unique_file_{i}.bin")
        Path(p).write_bytes(os.urandom(200 * 1024))
        file_paths.append(str(p))

    # Three pairs of identical files
    for i in range(3):
        content = os.urandom(100 * 1024)
        for suffix in ("a", "b"):
            p = workdir(f"dup_{i}_{suffix}.bin")
            Path(p).write_bytes(content)
            file_paths.append(str(p))

    # Audio files for FFT analysis
    wav_paths = []
    for i in range(4):
        p = workdir(f"audio_{i}.wav")
        generate_real_audio_wav(p, duration_sec=2.0 + i * 0.5)
        wav_paths.append(p)

    print(f"Files for dedup: {len(file_paths)}, WAV files: {len(wav_paths)}")

    # Queue all tasks
    queue.push(
        Task(
            task_type="find_duplicate_files_by_hash", payload={"file_paths": file_paths}
        )
    )
    for wp in wav_paths:
        queue.push(
            Task(
                task_type="audio_fft_analysis",
                payload={"wav_path": wp, "top_n_frequencies": 8},
            )
        )
    for fp in file_paths[:4]:
        queue.push(Task(task_type="multi_algorithm_hash", payload={"file_path": fp}))

    worker = ThreadedWorker(max_workers=4)
    worker.start()


def test_full_media_chain():
    """
    End-to-end chain combining video, audio, and image processing:
    transcode video → extract audio → FFT analysis → spectrogram
    Tests ChainWorker passing results across heterogeneous task types.
    """
    print("\n=== TEST 7: Full Media Chain (Video + Audio + Signal) ===")

    src_video = workdir("media_source.mp4")
    hevc_out = workdir("media_hevc.mp4")
    audio_wav = workdir("media_audio.wav")
    filtered_wav = workdir("media_filtered.wav")
    spectrogram_out = workdir("media_spectrogram.png")

    generate_real_video(src_video, duration_sec=8, width=854, height=480)
    print(f"Source: {os.path.getsize(src_video) / 1e6:.2f} MB")

    worker = ChainWorker()
    chain_id = chain.chain(
        [
            {
                "task_type": "transcode_h264_to_hevc",
                "payload": {
                    "input_path": src_video,
                    "output_path": hevc_out,
                    "crf": 28,
                },
            },
            {
                "task_type": "extract_audio_from_video",
                "payload": {
                    "input_path": hevc_out,
                    "output_path": audio_wav,
                    "sample_rate": 44100,
                },
            },
            {
                "task_type": "audio_bandpass_filter",
                "payload": {
                    "wav_path": audio_wav,
                    "output_path": filtered_wav,
                    "low_hz": 200,
                    "high_hz": 4000,
                    "order": 4,
                },
            },
            {
                "task_type": "generate_spectrogram",
                "payload": {
                    "wav_path": filtered_wav,
                    "output_path": spectrogram_out,
                    "fft_size": 2048,
                },
            },
            {
                "task_type": "audio_fft_analysis",
                "payload": {"wav_path": filtered_wav, "top_n_frequencies": 20},
            },
        ],
        completion_task="on_chain_finished",
    )
    print(f"Chain ID: {chain_id}")
    worker.start()


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------


def run_all():
    print("\n" + "=" * 60)
    print("BROCCOLI INTENSIVE TEST SUITE")
    print(f"Work dir: {WORKDIR}")
    print("=" * 60)

    tests = [
        test_video_transcode_pipeline,
        test_audio_processing_chain,
        test_image_ml_pipeline,
        test_database_pipeline,
        test_file_integrity_pool,
        test_duplicate_detection_and_audio_analysis,
        test_full_media_chain,
    ]

    for test in tests:
        try:
            test()
        except Exception as e:
            print(f"[ERROR] {test.__name__}: {e}")
        time.sleep(0.5)

    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETED")
    print(f"Outputs in: {WORKDIR}")
    print("=" * 60)


if __name__ == "__main__":
    RedisController().delete_all_keys()  # Clean up Redis before tests
    # Run a single test or all:
    run_all()
    # test_full_media_chain()
