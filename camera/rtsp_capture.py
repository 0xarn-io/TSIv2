"""
rtsp_capture.py - Low-level RTSP frame capture using OpenCV.
"""

import cv2
import os
import base64
import numpy as np
from typing import Optional


def capture_frame(
    rtsp_url: str,
    use_tcp: bool = True,
    warmup_frames: int = 5,
    timeout_ms: int = 5000,
) -> Optional[np.ndarray]:
    """Capture a single frame from an RTSP stream as a NumPy array (BGR)."""
    if use_tcp:
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
            f"rtsp_transport;tcp|stimeout;{timeout_ms * 1000}"
        )

    cap = cv2.VideoCapture(rtsp_url, cv2.CAP_FFMPEG)
    cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)

    if not cap.isOpened():
        return None

    try:
        for _ in range(warmup_frames):
            cap.read()

        ret, frame = cap.read()
        return frame if ret else None
    finally:
        cap.release()


def capture_as_jpeg_bytes(
    rtsp_url: str,
    quality: int = 90,
    **kwargs,
) -> Optional[bytes]:
    """Capture a snapshot and return JPEG-encoded bytes."""
    frame = capture_frame(rtsp_url, **kwargs)
    if frame is None:
        return None

    success, encoded = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    return encoded.tobytes() if success else None


def capture_as_base64(
    rtsp_url: str,
    quality: int = 90,
    as_data_url: bool = True,
    **kwargs,
) -> Optional[str]:
    """Capture a snapshot and return it as a base64 string (or data URL)."""
    jpeg_bytes = capture_as_jpeg_bytes(rtsp_url, quality=quality, **kwargs)
    if jpeg_bytes is None:
        return None

    b64 = base64.b64encode(jpeg_bytes).decode("ascii")
    return f"data:image/jpeg;base64,{b64}" if as_data_url else b64