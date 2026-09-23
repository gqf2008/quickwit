// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Shared infrastructure for indexing pipeline supervisors (logs and metrics).

use std::time::{Duration, Instant};

use tokio::sync::Semaphore;

pub(crate) const SUPERVISE_INTERVAL: Duration = Duration::from_secs(1);

const MAX_RETRY_DELAY: Duration = Duration::from_mins(10);

/// How long a pipeline has to stay healthy before its restart backoff is considered recovered.
///
/// Without a reset, a pipeline that failed once during an outage would keep paying the last delay,
/// up to [`MAX_RETRY_DELAY`], long after the storage came back.
pub(crate) const RESTART_BACKOFF_RESET_DELAY: Duration = Duration::from_secs(60);

#[derive(Debug)]
pub(crate) struct SuperviseLoop;

/// Calculates the wait time based on retry count.
// retry_count, wait_time
// 0   1s
// 1   2s
// 2   4s
// 3   8s
// ...
// >=8   5mn
pub(crate) fn wait_duration_before_retry(retry_count: usize) -> Duration {
    // Protect against a `retry_count` that will lead to an overflow.
    let max_power = (retry_count as u32).min(31);
    Duration::from_secs(2u64.pow(max_power)).min(MAX_RETRY_DELAY)
}

/// Tracks how many times a pipeline had to be restarted in a row, so that restarts back off.
///
/// Both spawn failures and failures that happen while the pipeline is running go through this
/// counter. A pipeline whose source dies because the storage is unreachable used to restart with a
/// fixed 1-second delay forever: the pipeline was spawning fine, so the spawn retry counter was
/// reset on every attempt and the exponential backoff never grew.
#[derive(Debug, Default)]
pub(crate) struct RestartBackoff {
    consecutive_failures: usize,
    healthy_since_opt: Option<Instant>,
}

impl RestartBackoff {
    /// Records a failure and returns the retry index to use for the next attempt.
    pub(crate) fn register_failure(&mut self) -> usize {
        let retry_count = self.consecutive_failures;
        self.consecutive_failures += 1;
        self.healthy_since_opt = None;
        retry_count
    }

    /// Records a failure and returns the retry index and the delay to wait before restarting.
    pub(crate) fn next_restart_delay(&mut self) -> (usize, Duration) {
        let retry_count = self.register_failure();
        (retry_count, wait_duration_before_retry(retry_count))
    }

    /// Records that the pipeline is healthy at `now`.
    ///
    /// Returns `true` when the backoff was reset because the pipeline stayed healthy for
    /// [`RESTART_BACKOFF_RESET_DELAY`].
    pub(crate) fn register_healthy(&mut self, now: Instant) -> bool {
        let healthy_since = *self.healthy_since_opt.get_or_insert(now);
        if self.consecutive_failures == 0 {
            return false;
        }
        if now.duration_since(healthy_since) >= RESTART_BACKOFF_RESET_DELAY {
            self.consecutive_failures = 0;
            self.healthy_since_opt = None;
            return true;
        }
        false
    }

    /// Carries the failure count of a failed spawn attempt into the running pipeline.
    ///
    /// Takes the maximum: the counter also covers failures that happen while the pipeline is
    /// running, and those must not be forgotten when a restart finally succeeds.
    pub(crate) fn carry_spawn_failures(&mut self, consecutive_failures: usize) {
        self.consecutive_failures = self.consecutive_failures.max(consecutive_failures);
        self.healthy_since_opt = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The regression that motivated this type: a pipeline dying at runtime used to restart with a
    /// fixed one-second delay, so a storage outage made every pipeline restart roughly every
    /// second.
    #[test]
    fn test_restart_delays_grow_on_consecutive_failures() {
        let mut backoff = RestartBackoff::default();
        let restarts: Vec<(usize, Duration)> =
            (0..5).map(|_| backoff.next_restart_delay()).collect();
        assert_eq!(
            restarts,
            vec![
                (0, Duration::from_secs(1)),
                (1, Duration::from_secs(2)),
                (2, Duration::from_secs(4)),
                (3, Duration::from_secs(8)),
                (4, Duration::from_secs(16)),
            ]
        );
    }

    #[test]
    fn test_restart_backoff_needs_sustained_health_to_reset() {
        let start = Instant::now();
        let mut backoff = RestartBackoff::default();
        assert_eq!(backoff.register_failure(), 0);
        assert_eq!(backoff.register_failure(), 1);
        // Being healthy once, or for less than the reset delay, keeps the backoff.
        assert!(!backoff.register_healthy(start));
        assert!(!backoff.register_healthy(start + RESTART_BACKOFF_RESET_DELAY / 2));
        // Staying healthy long enough resets it.
        assert!(backoff.register_healthy(start + RESTART_BACKOFF_RESET_DELAY));
        assert_eq!(backoff.register_failure(), 0);
    }

    #[test]
    fn test_restart_backoff_health_timer_restarts_after_a_failure() {
        let start = Instant::now();
        let mut backoff = RestartBackoff::default();
        assert_eq!(backoff.register_failure(), 0);
        assert!(!backoff.register_healthy(start));
        // A new failure restarts the health timer...
        backoff.register_failure();
        assert!(!backoff.register_healthy(start + RESTART_BACKOFF_RESET_DELAY));
        // ...so the counter only resets after a full reset delay of continuous health.
        assert!(backoff.register_healthy(start + RESTART_BACKOFF_RESET_DELAY * 2));
        assert_eq!(backoff.register_failure(), 0);
    }

    #[test]
    fn test_restart_backoff_carries_spawn_failures() {
        let mut backoff = RestartBackoff::default();
        backoff.carry_spawn_failures(3);
        assert_eq!(backoff.register_failure(), 3);
        // A restart that reports fewer failures must not lower the backoff.
        backoff.carry_spawn_failures(1);
        assert_eq!(backoff.register_failure(), 4);
        // A fresh pipeline (a new actor) starts from scratch.
        assert_eq!(RestartBackoff::default().register_failure(), 0);
    }
}

/// Spawning an indexing pipeline puts a lot of pressure on the file system, metastore, etc. so
/// we rely on this semaphore to limit the number of indexing pipelines that can be spawned
/// concurrently.
/// See also <https://github.com/quickwit-oss/quickwit/issues/1638>.
pub(crate) static SPAWN_PIPELINE_SEMAPHORE: Semaphore = Semaphore::const_new(10);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Spawn {
    pub(crate) retry_count: usize,
}

// ---------------------------------------------------------------------------
// Pipeline trait — type-erased handle for any indexing pipeline actor
// ---------------------------------------------------------------------------

use async_trait::async_trait;
use quickwit_actors::{
    Actor, ActorExitStatus, ActorHandle, ActorState, DeferableReplyHandler, Health, Mailbox,
    Observation, SendError, Supervisable,
};
use quickwit_proto::indexing::IndexingPipelineId;

use crate::models::IndexingStatistics;
use crate::source::AssignShards;

/// Trait that abstracts over the concrete pipeline actor type
/// (`IndexingPipeline` or `MetricsPipeline`). This allows `PipelineHandle`
/// to hold a single `Box<dyn PipelineHandle>`.
#[async_trait]
pub trait PipelineHandle: Send + Sync {
    fn indexing_pipeline_id(&self) -> &IndexingPipelineId;
    fn state(&self) -> ActorState;
    fn refresh_observe(&self);
    fn last_observation(&self) -> IndexingStatistics;
    fn check_health(&self, check_for_progress: bool) -> Health;
    async fn send_assign_shards(&self, message: AssignShards) -> Result<(), SendError>;
    async fn observe(&self) -> Observation<IndexingStatistics>;
    async fn join(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics);
    async fn quit(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics);
    async fn kill(self: Box<Self>);
}

/// Generic wrapper that implements `PipelineHandle` for any actor with the right
/// observable state and message handlers.
pub(crate) struct ActorPipeline<A: Actor<ObservableState = IndexingStatistics>> {
    pub pipeline_id: IndexingPipelineId,
    pub mailbox: Mailbox<A>,
    pub handle: ActorHandle<A>,
}

#[async_trait]
impl<A> PipelineHandle for ActorPipeline<A>
where A: Actor<ObservableState = IndexingStatistics> + DeferableReplyHandler<AssignShards>
{
    fn indexing_pipeline_id(&self) -> &IndexingPipelineId {
        &self.pipeline_id
    }

    fn state(&self) -> ActorState {
        self.handle.state()
    }

    fn refresh_observe(&self) {
        self.handle.refresh_observe();
    }

    fn last_observation(&self) -> IndexingStatistics {
        self.handle.last_observation().clone()
    }

    fn check_health(&self, check_for_progress: bool) -> Health {
        self.handle.check_health(check_for_progress)
    }

    async fn send_assign_shards(&self, message: AssignShards) -> Result<(), SendError> {
        self.mailbox.send_message(message).await?;
        Ok(())
    }

    async fn observe(&self) -> Observation<IndexingStatistics> {
        self.handle.observe().await
    }

    async fn join(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics) {
        self.handle.join().await
    }

    async fn quit(self: Box<Self>) -> (ActorExitStatus, IndexingStatistics) {
        self.handle.quit().await
    }

    async fn kill(self: Box<Self>) {
        let _ = self.handle.kill().await;
    }
}
