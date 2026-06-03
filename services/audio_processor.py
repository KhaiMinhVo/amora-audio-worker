"""
services/audio_processor.py
Module khử nhiễu & chuẩn hóa âm lượng bằng FFmpeg.
"""
import ffmpeg
import os


def clean_audio_with_ffmpeg(input_path: str, output_path: str) -> bool:
    """
    Lọc nhiễu môi trường và chuẩn hóa âm lượng (loudness normalization).

    Filters:
        - afftdn (nf=-25): Adaptive Fast Fourier Transform Denoiser, ngưỡng lọc nhiễu -25 dB.
        - loudnorm: EBU R128 loudness normalization.
            I=-16  : Integrated loudness target (LUFS).
            LRA=11 : Loudness range target.
            TP=-1.5: True peak ceiling (dBFS).

    Returns:
        True nếu thành công, False nếu có lỗi.
    """
    try:
        (
            ffmpeg
            .input(input_path)
            .filter('afftdn', nf='-25')
            .filter('loudnorm', I=-16, LRA=11, TP=-1.5)
            .output(output_path, acodec='aac', audio_bitrate='64k')
            .overwrite_output()
            .run(capture_stdout=True, capture_stderr=True)
        )
        return True
    except ffmpeg.Error as e:
        print(f'[audio_processor] Lỗi FFmpeg: {e.stderr.decode()}')
        return False
