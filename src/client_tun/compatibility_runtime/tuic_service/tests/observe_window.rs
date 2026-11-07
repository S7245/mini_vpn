//! O4 (D28) tracers for the starvation-compensated observation window used by
//! every fixed-5s Service loopback observation. Std-thread sleep blocks the
//! observing thread without consuming its CPU, which is exactly how OS
//! starvation appears to the window; a spin consumes CPU and must not extend.
use super::fixture::{progress_window, window, ProgressElapsed, WindowElapsed};
use std::time::Duration;

const BOUND: Duration = Duration::from_millis(150);
const CHUNK: Duration = Duration::from_millis(40);

#[tokio::test]
async fn idle_unfinished_observation_still_fails_at_the_original_bound() {
    let begin = std::time::Instant::now();
    let result = window(BOUND, std::future::pending::<()>()).await;
    let WindowElapsed { elapsed, .. } = result.unwrap_err();
    assert!(elapsed >= BOUND && begin.elapsed() >= BOUND);
}

async fn blocked_progress(chunks: u32) -> u32 {
    for _ in 0..chunks {
        std::thread::sleep(CHUNK);
        tokio::task::yield_now().await;
    }
    chunks
}

#[tokio::test]
async fn starved_observing_thread_is_compensated_where_fixed_timeout_fails() {
    // 8 × 40ms of thread starvation, twice the bound: the plain fixed window
    // reports Elapsed although the work only lacked CPU; the window admits it.
    assert!(tokio::time::timeout(BOUND, blocked_progress(8)).await.is_err());
    assert_eq!(window(BOUND, blocked_progress(8)).await.unwrap(), 8);
}

#[tokio::test]
async fn busy_self_work_is_not_credited_and_fails_closed() {
    let spin = async {
        loop {
            // Long non-yielding chunks make every tick ~90% late by own CPU.
            let until = std::time::Instant::now() + CHUNK * 5;
            while std::time::Instant::now() < until {
                std::hint::spin_loop();
            }
            tokio::task::yield_now().await;
        }
    };
    let elapsed = window(BOUND, spin).await.unwrap_err().elapsed;
    // Correct: bound + one chunk (~350ms). Crediting own CPU extends ~90% of
    // every tick and runs to ~10× bound.
    assert!(elapsed < BOUND * 6, "{elapsed:?}");
}

#[tokio::test(start_paused = true)]
async fn paused_clock_keeps_exact_fixed_window_semantics() {
    let five = Duration::from_secs(5);
    assert!(window(five, tokio::time::sleep(Duration::from_secs(4))).await.is_ok());
    let late = window(five, tokio::time::sleep(Duration::from_secs(6))).await.unwrap_err();
    assert_eq!(late.starved, Duration::ZERO);
    assert!(late.elapsed >= five && late.elapsed < Duration::from_secs(6));
}

// D47 R2 (D33 seam): progress-stall window tracers.
fn advance(progress: &std::cell::Cell<u64>) { progress.set(progress.get() + 1); }

#[tokio::test]
async fn progress_window_idle_fails_at_the_stall_bound() {
    let progress = std::cell::Cell::new(0);
    let err = progress_window(BOUND, &progress, std::future::pending::<()>()).await.unwrap_err();
    let ProgressElapsed::Stalled { progress: 0, since_progress, .. } = err else { panic!("{err:?}") };
    assert!(since_progress >= BOUND && since_progress < BOUND * 3, "{since_progress:?}");
}

#[tokio::test(start_paused = true)]
async fn progress_window_slow_monotone_progress_passes_far_beyond_one_stall() {
    let progress = std::cell::Cell::new(0);
    let five = Duration::from_secs(5);
    let work = async {
        for _ in 0..20 { tokio::time::sleep(Duration::from_secs(1)).await; advance(&progress); }
        20
    };
    assert_eq!(progress_window(five, &progress, work).await.unwrap(), 20);
}

#[tokio::test(start_paused = true)]
async fn progress_window_progress_then_stop_fails_one_stall_after_last_progress() {
    let progress = std::cell::Cell::new(0);
    let five = Duration::from_secs(5);
    let begin = tokio::time::Instant::now();
    let work = async {
        for _ in 0..3 { tokio::time::sleep(Duration::from_secs(2)).await; advance(&progress); }
        std::future::pending::<()>().await
    };
    let err = progress_window(five, &progress, work).await.unwrap_err();
    let ProgressElapsed::Stalled { progress: 3, since_progress, starved } = err else { panic!("{err:?}") };
    assert_eq!(starved, Duration::ZERO);
    assert!(since_progress >= five && since_progress < five + Duration::from_millis(100), "{since_progress:?}");
    let total = begin.elapsed();
    assert!(total >= Duration::from_secs(11) && total < Duration::from_secs(12), "{total:?}");
}

#[tokio::test(start_paused = true)]
async fn progress_window_endless_progress_fails_closed_at_the_cap() {
    let progress = std::cell::Cell::new(0);
    let five = Duration::from_secs(5);
    let work = async { loop { tokio::time::sleep(Duration::from_secs(1)).await; advance(&progress); } };
    let err = progress_window(five, &progress, work).await.unwrap_err();
    let ProgressElapsed::Cap { elapsed, progress: p } = err else { panic!("{err:?}") };
    assert!(elapsed >= five * 12 && elapsed < five * 12 + Duration::from_secs(1) && p >= 59, "{elapsed:?} {p}");
}

#[tokio::test(start_paused = true)]
async fn progress_window_paused_clock_without_progress_equals_fixed_timeout() {
    let progress = std::cell::Cell::new(0);
    let five = Duration::from_secs(5);
    assert!(progress_window(five, &progress, tokio::time::sleep(Duration::from_secs(4))).await.is_ok());
    let err = progress_window(five, &progress, tokio::time::sleep(Duration::from_secs(6))).await.unwrap_err();
    assert!(matches!(err, ProgressElapsed::Stalled { since_progress, starved: Duration::ZERO, .. } if since_progress >= five && since_progress < Duration::from_secs(6)));
}
