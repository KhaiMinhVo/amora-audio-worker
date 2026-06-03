"""
services/pet_analyzer.py
Module phân tích đặc trưng giọng nói (Voice Vibe) để khởi tạo Pet của người dùng.
Hoàn toàn Zero-Knowledge: chỉ đọc dạng sóng âm vật lý, không chuyển thành văn bản.
"""
import librosa
import numpy as np


def extract_voice_vibe(file_path: str) -> dict:
    """
    Trích xuất các đặc trưng vật lý từ file âm thanh.

    Returns:
        dict chứa:
            energy         (float): RMS trung bình — đại diện độ "mạnh mẽ / sôi nổi".
            pitch          (float): Cao độ cơ bản trung bình (Hz) — giọng trầm hay bổng.
            pitch_variance (float): Phương sai của cao độ — nói có ngữ điệu hay đọc đều đều.
            is_monotone    (bool) : True nếu pitch_variance < 500 → Pet sẽ phản ứng "buồn".
            duration_sec   (float): Độ dài file (giây).
    """
    # Tải file, giữ nguyên sample rate gốc (sr=None)
    y, sr = librosa.load(file_path, sr=None, mono=True)

    # --- 1. Energy (RMS) ---
    rms = librosa.feature.rms(y=y)
    mean_energy = float(np.mean(rms))

    # --- 2. Pitch (F0) via pYIN ---
    # fmin / fmax bao phủ toàn dải giọng người (C2 ≈ 65 Hz → C7 ≈ 2093 Hz)
    f0, voiced_flag, voiced_probs = librosa.pyin(
        y,
        fmin=librosa.note_to_hz('C2'),
        fmax=librosa.note_to_hz('C7'),
        sr=sr
    )
    f0_voiced = f0[~np.isnan(f0)]  # Bỏ các frame lặng (NaN)

    mean_pitch = float(np.mean(f0_voiced)) if len(f0_voiced) > 0 else 0.0
    pitch_variance = float(np.var(f0_voiced)) if len(f0_voiced) > 0 else 0.0

    # --- 3. Duration ---
    duration_sec = float(librosa.get_duration(y=y, sr=sr))

    return {
        "energy": round(mean_energy, 6),
        "pitch": round(mean_pitch, 2),
        "pitch_variance": round(pitch_variance, 2),
        "is_monotone": pitch_variance < 500,
        "duration_sec": round(duration_sec, 2),
    }
