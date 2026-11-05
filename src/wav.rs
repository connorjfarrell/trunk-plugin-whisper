//! The recorder's WAV (16-bit mono, 8 kHz) as the 16 kHz WAV Whisper takes.

use std::f64::consts::PI;

const TARGET: u32 = 16_000;

/// A WAV's samples and rate. Only 16-bit PCM, mono: what the recorder writes.
pub fn read(wav: &[u8]) -> Result<(Vec<i16>, u32), String> {
    if wav.len() < 12 || &wav[0..4] != b"RIFF" || &wav[8..12] != b"WAVE" {
        return Err("not a WAV file".into());
    }
    let mut rate = 0;
    let mut pos = 12;
    while pos + 8 <= wav.len() {
        let id = &wav[pos..pos + 4];
        let size = u32::from_le_bytes(wav[pos + 4..pos + 8].try_into().unwrap()) as usize;
        let body = pos + 8;
        match id {
            b"fmt " => {
                if body + 16 > wav.len() {
                    return Err("the WAV's format is cut short".into());
                }
                let tag = u16::from_le_bytes(wav[body..body + 2].try_into().unwrap());
                let channels = u16::from_le_bytes(wav[body + 2..body + 4].try_into().unwrap());
                rate = u32::from_le_bytes(wav[body + 4..body + 8].try_into().unwrap());
                let bits = u16::from_le_bytes(wav[body + 14..body + 16].try_into().unwrap());
                if tag != 1 || channels != 1 || bits != 16 {
                    return Err(format!("expected 16-bit mono PCM, got format {tag}, {channels} channel(s), {bits} bits"));
                }
            }
            b"data" => {
                if rate == 0 {
                    return Err("the WAV's data comes before its format".into());
                }
                // A streamed file can claim more than it holds: take what's there.
                let end = body.saturating_add(size).min(wav.len());
                let samples = wav[body..end].as_chunks::<2>().0.iter().map(|b| i16::from_le_bytes(*b)).collect();
                return Ok((samples, rate));
            }
            _ => {}
        }
        pos = body.saturating_add(size).saturating_add(size & 1);
    }
    Err("the WAV has no audio".into())
}

/// Weights for the sample half way between two: windowed sinc over `L` samples each side.
fn half_way_taps() -> Vec<f64> {
    const L: i32 = 8;
    let mut taps: Vec<f64> = (1 - L..=L)
        .map(|j| {
            let t = j as f64 - 0.5;
            let sinc = (PI * t).sin() / (PI * t);
            let hann = 0.5 * (1.0 + (PI * t / L as f64).cos());
            sinc * hann
        })
        .collect();
    let sum: f64 = taps.iter().sum();
    taps.iter_mut().for_each(|t| *t /= sum);
    taps
}

/// The samples at 16 kHz. 8 kHz doubles with band-limited interpolation; any
/// other rate is interpolated linearly (the recorder doesn't write one).
pub fn to_16k(samples: &[i16], rate: u32) -> Vec<i16> {
    if rate == TARGET {
        return samples.to_vec();
    }
    if rate == TARGET / 2 {
        let taps = half_way_taps();
        let n = samples.len();
        let at = |i: i64| if i < 0 || i >= n as i64 { 0.0 } else { samples[i as usize] as f64 };
        let mut out = Vec::with_capacity(n * 2);
        for i in 0..n as i64 {
            out.push(samples[i as usize]);
            let v: f64 = taps.iter().enumerate().map(|(k, w)| w * at(i + k as i64 - 7)).sum();
            out.push(v.round().clamp(-32768.0, 32767.0) as i16);
        }
        return out;
    }
    let n = (samples.len() as u64 * TARGET as u64 / rate.max(1) as u64) as usize;
    (0..n)
        .map(|i| {
            let x = i as f64 * rate as f64 / TARGET as f64;
            let (a, f) = (x.floor() as usize, x - x.floor());
            let s0 = *samples.get(a).unwrap_or(&0) as f64;
            let s1 = *samples.get(a + 1).unwrap_or(&(s0 as i16)) as f64;
            (s0 + (s1 - s0) * f).round() as i16
        })
        .collect()
}

/// A mono 16-bit WAV.
pub fn write(samples: &[i16], rate: u32) -> Vec<u8> {
    let data = samples.len() as u32 * 2;
    let mut w = Vec::with_capacity(44 + data as usize);
    w.extend_from_slice(b"RIFF");
    w.extend_from_slice(&(36 + data).to_le_bytes());
    w.extend_from_slice(b"WAVEfmt ");
    w.extend_from_slice(&16u32.to_le_bytes());
    w.extend_from_slice(&1u16.to_le_bytes());
    w.extend_from_slice(&1u16.to_le_bytes());
    w.extend_from_slice(&rate.to_le_bytes());
    w.extend_from_slice(&(rate * 2).to_le_bytes());
    w.extend_from_slice(&2u16.to_le_bytes());
    w.extend_from_slice(&16u16.to_le_bytes());
    w.extend_from_slice(b"data");
    w.extend_from_slice(&data.to_le_bytes());
    for s in samples {
        w.extend_from_slice(&s.to_le_bytes());
    }
    w
}

/// Where to cut `samples` into pieces of at most `max_s` seconds: at the last pause (a stretch of at least 160 ms
/// much quieter than the speech) before the limit, else at the limit. Pieces are `(start, end)` sample numbers; any
/// piece under 0.3 s is dropped (nothing was said in it).
pub fn split_at_pauses(samples: &[i16], rate: u32, max_s: f64) -> Vec<(usize, usize)> {
    let frame = (rate / 50).max(1) as usize; // 20 ms
    let rms: Vec<f64> = samples.chunks(frame).map(|c| (c.iter().map(|&v| (v as f64).powi(2)).sum::<f64>() / c.len() as f64).sqrt()).collect();
    let mut sorted = rms.clone();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let loud = sorted.get(sorted.len() * 9 / 10).copied().unwrap_or(0.0);
    let quiet_below = (0.2 * loud).max(50.0);
    // The middle of each run of quiet frames at least 160 ms long, as a sample number.
    let mut pauses = Vec::new();
    let mut i = 0;
    while i < rms.len() {
        if rms[i] < quiet_below {
            let start = i;
            while i < rms.len() && rms[i] < quiet_below {
                i += 1;
            }
            if i - start >= 8 {
                pauses.push((start + i) / 2 * frame);
            }
        } else {
            i += 1;
        }
    }
    let (max_len, min_len) = ((max_s * rate as f64) as usize, 3 * rate as usize);
    let (mut pieces, mut pos) = (Vec::new(), 0);
    while pos < samples.len() {
        if samples.len() - pos <= max_len {
            pieces.push((pos, samples.len()));
            break;
        }
        let cut = pauses.iter().copied().filter(|&p| p > pos + min_len && p <= pos + max_len).max().unwrap_or(pos + max_len);
        pieces.push((pos, cut));
        pos = cut;
    }
    pieces.retain(|&(a, b)| (b - a) as f64 >= 0.3 * rate as f64);
    pieces
}

/// What goes to the server: the recorder's WAV, at 16 kHz, whole (`split_s` `None`) or cut into pieces of at most
/// `split_s` seconds at pauses. Each piece comes with where it starts, in seconds.
pub fn pieces_for_whisper(wav: &[u8], split_s: Option<f64>) -> Result<Vec<(f64, Vec<u8>)>, String> {
    let (samples, rate) = read(wav)?;
    if samples.is_empty() {
        return Err("the WAV has no audio".into());
    }
    let up = to_16k(&samples, rate);
    match split_s {
        None => Ok(vec![(0.0, write(&up, TARGET))]),
        Some(max) => {
            let pieces = split_at_pauses(&up, TARGET, max);
            if pieces.is_empty() {
                return Err("the WAV has no audio".into());
            }
            Ok(pieces.into_iter().map(|(a, b)| (a as f64 / TARGET as f64, write(&up[a..b], TARGET))).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(hz: f64, rate: u32, n: usize) -> Vec<i16> {
        (0..n).map(|i| (12000.0 * (2.0 * PI * hz * i as f64 / rate as f64).sin()) as i16).collect()
    }

    fn rms(s: &[i16]) -> f64 {
        (s.iter().map(|&v| (v as f64).powi(2)).sum::<f64>() / s.len() as f64).sqrt()
    }

    #[test]
    fn a_wav_round_trips() {
        let s = tone(440.0, 8000, 800);
        assert_eq!(read(&write(&s, 8000)).unwrap(), (s, 8000));
    }

    #[test]
    fn eight_khz_doubles_and_keeps_a_tone_at_the_same_level_and_pitch() {
        let s = tone(1000.0, 8000, 4000);
        let up = to_16k(&s, 8000);
        assert_eq!(up.len(), 8000);
        // Level kept (the edges are left out: the filter has nothing beyond them).
        let (a, b) = (rms(&s[100..3900]), rms(&up[200..7800]));
        assert!((a - b).abs() / a < 0.02, "{a} vs {b}");
        // The new samples sit on the same sine.
        let want = tone(1000.0, 16000, 8000);
        let err = (200..7800).map(|i| (up[i] as f64 - want[i] as f64).abs()).fold(0.0, f64::max);
        assert!(err < 400.0, "worst error {err}");
    }

    #[test]
    fn other_rates_and_16k_pass_through_sensibly() {
        let s = tone(300.0, 16000, 1600);
        assert_eq!(to_16k(&s, 16000), s);
        let from_12k = to_16k(&tone(300.0, 12000, 1200), 12000);
        assert_eq!(from_12k.len(), 1600);
    }

    #[test]
    fn what_isnt_a_mono_16_bit_wav_is_refused_with_a_reason() {
        assert!(read(b"nope").is_err());
        let mut stereo = write(&[0; 8], 8000);
        stereo[22] = 2;
        assert!(read(&stereo).unwrap_err().contains("2 channel"));
        assert!(pieces_for_whisper(&write(&[], 8000), None).is_err());
    }

    /// Speech-like bursts (a tone, loud) with pauses (near silence) between, `secs` long altogether.
    fn bursts(burst_s: f64, pause_s: f64, secs: f64) -> Vec<i16> {
        let rate = 16000.0;
        let mut out = Vec::new();
        while (out.len() as f64) < secs * rate {
            out.extend(tone(300.0, 16000, (burst_s * rate) as usize));
            out.extend(std::iter::repeat_n(3i16, (pause_s * rate) as usize));
        }
        out.truncate((secs * rate) as usize);
        out
    }

    #[test]
    fn a_long_call_is_cut_at_pauses_into_pieces_that_fit_and_nothing_is_lost() {
        let s = bursts(2.5, 0.4, 30.0);
        let pieces = split_at_pauses(&s, 16000, 12.0);
        assert!(pieces.len() >= 3, "{pieces:?}");
        let mut at = 0;
        for &(a, b) in &pieces {
            assert_eq!(a, at, "pieces are back to back");
            assert!((b - a) as f64 <= 12.0 * 16000.0, "a piece is {} s", (b - a) as f64 / 16000.0);
            at = b;
        }
        assert_eq!(at, s.len());
        // Each cut lands in a pause, not in the middle of a burst.
        for &(_, b) in &pieces[..pieces.len() - 1] {
            assert!(s[b].abs() <= 3, "cut at {b} is inside a burst");
        }
    }

    #[test]
    fn a_short_call_is_one_piece_and_continuous_sound_is_cut_at_the_limit() {
        let one = bursts(2.5, 0.4, 8.0);
        assert_eq!(split_at_pauses(&one, 16000, 12.0), vec![(0, one.len())]);
        let steady = tone(300.0, 16000, 30 * 16000);
        let pieces = split_at_pauses(&steady, 16000, 12.0);
        assert_eq!(pieces, vec![(0, 192_000), (192_000, 384_000), (384_000, 480_000)]);
    }

    #[test]
    fn pieces_come_with_their_start_times_and_a_sliver_is_dropped() {
        let wav = write(&bursts(2.5, 0.4, 20.0).iter().step_by(2).copied().collect::<Vec<_>>(), 8000);
        let whole = pieces_for_whisper(&wav, None).unwrap();
        assert_eq!((whole.len(), whole[0].0), (1, 0.0));
        let cut = pieces_for_whisper(&wav, Some(12.0)).unwrap();
        assert!(cut.len() >= 2 && cut[0].0 == 0.0 && cut[1].0 > 3.0 && cut[1].0 <= 12.0, "{:?}", cut.iter().map(|p| p.0).collect::<Vec<_>>());
        // 12.1 s of sound at a 12 s limit: the 0.1 s left over is not sent.
        let steady = write(&tone(300.0, 8000, (12.1 * 8000.0) as usize), 8000);
        assert_eq!(pieces_for_whisper(&steady, Some(12.0)).unwrap().len(), 1);
    }

    #[test]
    fn a_streamed_wav_with_a_too_long_data_size_reads_what_there_is() {
        let mut w = write(&[1, 2, 3, 4], 8000);
        w[40..44].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(read(&w).unwrap().0, vec![1, 2, 3, 4]);
    }
}
