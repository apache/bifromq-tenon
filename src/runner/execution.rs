/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Contains the policy, change notifications, and the Runner's entitlement deadline.

use crate::runner::extensions::{ExecutionDenied, ExecutionPolicy, ExecutionScope, PolicyChanges};
use std::future::{self, Future};
use std::time::Instant;
use tokio::sync::watch;
use tokio::time;

pub(super) struct RunnerExecution {
    policy: Box<dyn ExecutionPolicy>,
    changes: Option<PolicyChanges>,
    expiry: ExecutionExpiry,
}

impl RunnerExecution {
    pub(super) fn new(policy: Box<dyn ExecutionPolicy>) -> Self {
        Self {
            changes: policy.changes(),
            policy,
            expiry: ExecutionExpiry::default(),
        }
    }

    pub(super) fn check(&self, scope: ExecutionScope<'_>) -> Result<(), ExecutionDenied> {
        Self::check_policy(self.policy.as_ref(), &self.expiry, scope)
    }

    /// Gives Pipeline supervision a handle to the same deadline.
    pub(super) fn expiry(&self) -> ExecutionExpiry {
        self.expiry.clone()
    }

    pub(super) async fn wait_for_expiry(&self) {
        self.expiry.wait_for_expiry().await;
    }

    /// The event loop can wait for changes and do policy checks at the same time.
    pub(super) fn split(
        &mut self,
    ) -> (
        impl Fn(ExecutionScope<'_>) -> Result<(), ExecutionDenied> + '_,
        impl Future<Output = bool> + '_,
        &ExecutionExpiry,
    ) {
        let policy = self.policy.as_ref();
        let expiry = &self.expiry;
        (
            move |scope| Self::check_policy(policy, expiry, scope),
            Self::policy_changed(&mut self.changes),
            expiry,
        )
    }

    fn check_policy(
        policy: &dyn ExecutionPolicy,
        expiry: &ExecutionExpiry,
        scope: ExecutionScope<'_>,
    ) -> Result<(), ExecutionDenied> {
        let decision = policy.authorize(scope);
        expiry.update(match &decision {
            Ok(permit) => permit.entitlement_until,
            Err(denied) => denied.entitlement_until,
        });
        decision.map(|_| ())
    }

    async fn policy_changed(changes: &mut Option<PolicyChanges>) -> bool {
        let Some(receiver) = changes else {
            return future::pending().await;
        };
        if receiver.changed().await.is_ok() {
            true
        } else {
            *changes = None;
            eprintln!(
                "runner.policy_changes_closed: Policy notifications stopped; the current deadline does not change"
            );
            false
        }
    }
}

#[derive(Clone)]
pub(super) struct ExecutionExpiry {
    deadline: watch::Sender<Option<Instant>>,
}

impl ExecutionExpiry {
    pub(super) fn is_expired(&self) -> bool {
        self.deadline
            .borrow()
            .is_some_and(|deadline| time::Instant::now().into_std() >= deadline)
    }

    pub(super) async fn wait_for_expiry(&self) {
        let mut updates = self.deadline.subscribe();
        loop {
            let deadline = *updates.borrow_and_update();
            if self.is_expired() {
                return;
            }
            tokio::select! {
                biased;
                changed = updates.changed() => {
                    assert!(changed.is_ok(), "Expiry owner remains alive while waiting");
                }
                () = async {
                    match deadline {
                        Some(deadline) => time::sleep_until(deadline.into()).await,
                        None => future::pending().await,
                    }
                } => return,
            }
        }
    }

    fn update(&self, entitlement_until: Option<Instant>) {
        self.deadline.send_replace(entitlement_until);
    }
}

impl Default for ExecutionExpiry {
    fn default() -> Self {
        Self {
            deadline: watch::channel(None).0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::extensions::ExecutionPermit;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::thread;
    use std::time::{Duration, Instant};
    use tokio::task;

    struct NotifiedPolicy {
        state: Rc<RefCell<Result<ExecutionPermit, ExecutionDenied>>>,
        receiver: PolicyChanges,
        subscriptions: Rc<Cell<usize>>,
    }

    impl ExecutionPolicy for NotifiedPolicy {
        fn authorize(
            &self,
            _scope: ExecutionScope<'_>,
        ) -> Result<ExecutionPermit, ExecutionDenied> {
            self.state.borrow().clone()
        }

        fn changes(&self) -> Option<PolicyChanges> {
            self.subscriptions.set(self.subscriptions.get() + 1);
            Some(self.receiver.clone())
        }
    }

    #[tokio::test(start_paused = true)]
    async fn notifications_survive_cancellation_and_use_the_latest_state()
    -> Result<(), ExecutionDenied> {
        let (updates, receiver) = watch::channel(());
        let state = Rc::new(RefCell::new(Ok(ExecutionPermit {
            entitlement_until: Some((time::Instant::now() + Duration::from_secs(1)).into_std()),
        })));
        let subscriptions = Rc::new(Cell::new(0));
        let mut execution = RunnerExecution::new(Box::new(NotifiedPolicy {
            state: Rc::clone(&state),
            receiver,
            subscriptions: Rc::clone(&subscriptions),
        }));
        execution.check(ExecutionScope { documents: &[] })?;
        {
            let (check, policy_changed, _) = execution.split();
            tokio::select! {
                biased;
                _ = policy_changed => panic!("No update was sent"),
                result = async {
                    task::yield_now().await;
                    check(ExecutionScope { documents: &[] })
                } => result?,
            }
        }
        *state.borrow_mut() = Err(ExecutionDenied::new(
            "test.denied",
            "Execution is not permitted",
            None,
        ));
        updates.send_replace(());
        *state.borrow_mut() = Ok(ExecutionPermit::default());
        updates.send_replace(());
        {
            let (check, policy_changed, _) = execution.split();
            assert!(policy_changed.await);
            check(ExecutionScope { documents: &[] })?;
        }
        time::advance(Duration::from_secs(2)).await;
        assert!(!execution.expiry.is_expired());
        assert_eq!(subscriptions.get(), 1);
        let (_, policy_changed, _) = execution.split();
        tokio::select! {
            biased;
            _ = policy_changed => panic!("A merged update was read twice"),
            () = task::yield_now() => {},
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn closed_notifications_preserve_expiry_and_disable_the_wait()
    -> Result<(), ExecutionDenied> {
        let (updates, receiver) = watch::channel(());
        let state = Rc::new(RefCell::new(Ok(ExecutionPermit {
            entitlement_until: Some((time::Instant::now() + Duration::from_secs(1)).into_std()),
        })));
        let mut execution = RunnerExecution::new(Box::new(NotifiedPolicy {
            state,
            receiver,
            subscriptions: Rc::new(Cell::new(0)),
        }));
        execution.check(ExecutionScope { documents: &[] })?;
        drop(updates);
        let (_, policy_changed, _) = execution.split();
        assert!(!policy_changed.await);
        let (_, policy_changed, _) = execution.split();
        tokio::select! {
            biased;
            _ = policy_changed => panic!("Closed channel caused a ready loop"),
            () = task::yield_now() => {},
        }
        time::advance(Duration::from_secs(1)).await;
        assert!(execution.expiry.is_expired());
        Ok(())
    }

    struct Policy(Rc<RefCell<Result<ExecutionPermit, ExecutionDenied>>>);

    impl ExecutionPolicy for Policy {
        fn authorize(
            &self,
            _scope: ExecutionScope<'_>,
        ) -> Result<ExecutionPermit, ExecutionDenied> {
            self.0.borrow().clone()
        }
    }

    #[tokio::test]
    async fn check_uses_the_policy_result_when_the_callback_crosses_the_old_deadline()
    -> Result<(), ExecutionDenied> {
        struct CrossDeadline {
            deadline: Instant,
            result: Result<ExecutionPermit, ExecutionDenied>,
            first_call: Cell<bool>,
            callback_completed: Rc<Cell<bool>>,
        }
        impl ExecutionPolicy for CrossDeadline {
            fn authorize(
                &self,
                _scope: ExecutionScope<'_>,
            ) -> Result<ExecutionPermit, ExecutionDenied> {
                if self.first_call.replace(false) {
                    return Ok(ExecutionPermit {
                        entitlement_until: Some(self.deadline),
                    });
                }
                // Reproduce a synchronous decision that returns after the current deadline.
                thread::sleep(self.deadline.saturating_duration_since(Instant::now()));
                assert!(Instant::now() >= self.deadline);
                self.callback_completed.set(true);
                self.result.clone()
            }
        }
        for result in [
            Ok(ExecutionPermit::default()),
            Err(ExecutionDenied::new(
                "test.denied",
                "Candidate rejected",
                None,
            )),
        ] {
            let expected = result.as_ref().map(|_| ()).map_err(|error| error.clone());
            let deadline = Instant::now() + Duration::from_millis(100);
            let completed = Rc::new(Cell::new(false));
            let execution = RunnerExecution::new(Box::new(CrossDeadline {
                deadline,
                result,
                first_call: Cell::new(true),
                callback_completed: Rc::clone(&completed),
            }));
            execution.check(ExecutionScope { documents: &[] })?;
            let decision = execution.check(ExecutionScope { documents: &[] });
            assert!(completed.get());
            assert_eq!(decision, expected);
            assert_eq!(*execution.expiry.deadline.borrow(), None);
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn successful_check_updates_expiry_before_any_file_work() -> Result<(), ExecutionDenied> {
        let deadline = time::Instant::now() + Duration::from_secs(10);
        let result = Rc::new(RefCell::new(Ok(ExecutionPermit {
            entitlement_until: Some(deadline.into_std()),
        })));
        let execution = RunnerExecution::new(Box::new(Policy(result)));
        let observed = execution.expiry.deadline.subscribe();
        execution.check(ExecutionScope { documents: &[] })?;
        assert_eq!(*observed.borrow(), Some(deadline.into_std()));
        time::advance(Duration::from_secs(10)).await;
        tokio::select! {
            biased;
            () = execution.expiry.wait_for_expiry() => {},
            () = task::yield_now() => panic!("Expiry waited for a document commit"),
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn operation_outcome_is_independent_of_an_expired_entitlement() {
        for decision in decisions(Some(time::Instant::now().into_std())) {
            let expected = decision.clone().map(|_| ());
            let execution = RunnerExecution::new(Box::new(Policy(Rc::new(RefCell::new(decision)))));
            assert_eq!(execution.check(ExecutionScope { documents: &[] }), expected);
            assert!(execution.expiry.is_expired());
            execution.expiry.wait_for_expiry().await;
        }
    }

    fn decisions(
        entitlement_until: Option<Instant>,
    ) -> [Result<ExecutionPermit, ExecutionDenied>; 2] {
        [
            Ok(ExecutionPermit { entitlement_until }),
            Err(ExecutionDenied::new(
                "test.denied",
                "Candidate rejected",
                entitlement_until,
            )),
        ]
    }

    #[tokio::test(start_paused = true)]
    async fn expiry_observes_the_exact_deadline_before_the_timer_rounds_up()
    -> Result<(), ExecutionDenied> {
        let deadline = time::Instant::now() + Duration::from_nanos(1);
        let execution = RunnerExecution::new(Box::new(Policy(Rc::new(RefCell::new(Ok(
            ExecutionPermit {
                entitlement_until: Some(deadline.into_std()),
            },
        ))))));
        execution.check(ExecutionScope { documents: &[] })?;
        time::advance(Duration::from_nanos(1)).await;
        tokio::select! {
            biased;
            () = execution.expiry.wait_for_expiry() => {},
            () = task::yield_now() => panic!("Expired permission waited for timer rounding"),
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn both_outcomes_can_remove_the_current_deadline() -> Result<(), ExecutionDenied> {
        for decision in decisions(None) {
            let result = Rc::new(RefCell::new(Ok(ExecutionPermit {
                entitlement_until: Some((time::Instant::now() + Duration::from_secs(5)).into_std()),
            })));
            let execution = RunnerExecution::new(Box::new(Policy(Rc::clone(&result))));
            execution.check(ExecutionScope { documents: &[] })?;
            let expiry = execution.expiry.wait_for_expiry();
            tokio::pin!(expiry);
            tokio::select! {
                biased;
                () = &mut expiry => panic!("Permission expired before its deadline"),
                () = task::yield_now() => {},
            }
            let expected = decision.clone().map(|_| ());
            *result.borrow_mut() = decision;
            assert_eq!(execution.check(ExecutionScope { documents: &[] }), expected);
            time::advance(Duration::from_secs(30)).await;
            tokio::select! {
                biased;
                () = &mut expiry => panic!("Removed deadline still expired"),
                () = task::yield_now() => {},
            }
        }
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn both_outcomes_shorten_extend_and_introduce_deadlines() -> Result<(), ExecutionDenied> {
        for initial in [None, Some(5), Some(30)] {
            for outcome in decisions(None) {
                let deadline = time::Instant::now() + Duration::from_secs(10);
                let decision = match outcome {
                    Ok(_) => Ok(ExecutionPermit {
                        entitlement_until: Some(deadline.into_std()),
                    }),
                    Err(mut denied) => {
                        denied.entitlement_until = Some(deadline.into_std());
                        Err(denied)
                    }
                };
                let result = Rc::new(RefCell::new(Ok(ExecutionPermit {
                    entitlement_until: initial.map(|seconds| {
                        (time::Instant::now() + Duration::from_secs(seconds)).into_std()
                    }),
                })));
                let execution = RunnerExecution::new(Box::new(Policy(Rc::clone(&result))));
                execution.check(ExecutionScope { documents: &[] })?;
                let expiry = execution.expiry.wait_for_expiry();
                tokio::pin!(expiry);
                tokio::select! {
                    biased;
                    () = &mut expiry => panic!("Initial permission expired before its deadline"),
                    () = task::yield_now() => {},
                }
                let expected = decision.clone().map(|_| ());
                *result.borrow_mut() = decision;
                assert_eq!(execution.check(ExecutionScope { documents: &[] }), expected);
                time::advance(Duration::from_secs(9)).await;
                tokio::select! {
                    biased;
                    () = &mut expiry => panic!("Replacement permission expired early"),
                    () = task::yield_now() => {},
                }
                time::advance(Duration::from_secs(1)).await;
                tokio::select! {
                    biased;
                    () = &mut expiry => {},
                    () = task::yield_now() => panic!("Updated permission deadline was ignored"),
                }
            }
        }
        Ok(())
    }
}
