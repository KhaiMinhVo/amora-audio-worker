"""
processors/vibe_analyzer.py
DSP-only vibe score (-5..10) — không STT, không đọc nội dung.
"""
import librosa
import numpy as np


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def compute_vibe_score(file_path: str) -> dict:
    y, sr = librosa.load(file_path, sr=None, mono=True)
    duration_sec = float(librosa.get_duration(y=y, sr=sr))

    rms = librosa.feature.rms(y=y)
    energy_rms = float(np.mean(rms))

    f0, _, _ = librosa.pyin(
        y,
        fmin=librosa.note_to_hz("C2"),
        fmax=librosa.note_to_hz("C7"),
        sr=sr,
    )
    f0_voiced = f0[~np.isnan(f0)]
    pitch_variance = float(np.var(f0_voiced)) if len(f0_voiced) else 0.0

    onset_env = librosa.onset.onset_strength(y=y, sr=sr)
    tempo = float(librosa.feature.rhythm.tempo(onset_envelope=onset_env, sr=sr)[0])
    speech_rate = tempo / 60.0

    jitter = float(np.std(np.diff(f0_voiced))) if len(f0_voiced) > 2 else 0.0

    # Chuẩn hóa heuristic
    e_norm = _clamp(energy_rms * 40, 0, 1)
    pv_norm = _clamp(pitch_variance / 2000, 0, 1)
    sr_norm = _clamp(speech_rate / 3, 0, 1)
    j_norm = _clamp(1 - jitter / 50, 0, 1)

    weighted = 0.35 * e_norm + 0.30 * pv_norm + 0.20 * sr_norm + 0.15 * j_norm
    vibe_score = int(round(weighted * 15 - 5))

    return {
        "vibeScore": int(_clamp(vibe_score, -5, 10)),
        "energyRms": round(energy_rms, 6),
        "pitchVariance": round(pitch_variance, 2),
        "speechRate": round(speech_rate, 3),
        "jitter": round(jitter, 3),
        "durationSeconds": round(duration_sec, 2),
    }
