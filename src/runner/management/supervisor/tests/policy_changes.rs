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

use super::programs::program_supervisor;
use super::*;
use crate::runner::recovery::recover;
use crate::runner::test_support::write_document;
use std::fs;
use std::io::Write;
use std::sync::Mutex;

struct PausedDocumentWrite {
    started: Mutex<Option<oneshot::Sender<()>>>,
    release: Mutex<std_mpsc::Receiver<()>>,
}

impl ArtifactProtection for PausedDocumentWrite {
    fn protect(&self, source: &[u8], output: &mut dyn Write) -> io::Result<()> {
        if let Some(started) = self
            .started
            .lock()
            .map_err(|_| io::Error::other("Lock failed"))?
            .take()
        {
            let _ = started.send(());
            let _ = self
                .release
                .lock()
                .map_err(|_| io::Error::other("Lock failed"))?
                .recv();
        }
        output.write_all(source)
    }

    fn unprotect(&self, stored: &[u8], output: &mut dyn Write) -> io::Result<()> {
        output.write_all(stored)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn policy_notification_cannot_miss_a_document_commit() -> io::Result<()> {
    for during_write in [true, false] {
        let directory = tempfile::tempdir()?;
        drop(program_supervisor(directory.path())?);
        write_document(directory.path(), "first", document("first"))?;
        let config = load_config(directory.path())?;
        let layout = prepare_runner_state_directory(directory.path()).map_err(io::Error::other)?;
        let runtime = tempfile::tempdir()?;
        let (started, was_started) = oneshot::channel();
        let (release, released) = std_mpsc::channel();
        let protection: Arc<dyn ArtifactProtection> = Arc::new(PausedDocumentWrite {
            started: Mutex::new(Some(started)),
            release: Mutex::new(released),
        });
        let recovered =
            recover(&config, &layout, &protection, runtime.path()).map_err(io::Error::other)?;
        let recovered = RunnerManagementSupervisor::recover(
            directory.path(),
            config.script_vm_limits(),
            recovered,
            protection,
            None,
            |_| Ok(()),
        )?;
        let (mut supervisor, management, initial) = RunnerManagementSupervisor::start(
            recovered,
            config.tenon_document_verifier().map_err(io::Error::other)?,
        );
        assert_eq!(initial.len(), 1);
        let source = document("second");
        let put = management.put_document(
            TenonDocumentId::try_from("second").map_err(io::Error::other)?,
            PutDocumentPrecondition::Create,
            source.clone().into_bytes().into_boxed_slice(),
        );
        tokio::pin!(put);
        let mut old_decisions = Vec::new();
        tokio::select! {
            _ = supervisor.next_event(|scope| { old_decisions.push(scope.documents.len()); Ok(()) }) => {
                return Err(io::Error::other("Document commit completed before release"));
            }
            _ = &mut put => return Err(io::Error::other("PUT completed before release")),
            result = was_started => result.map_err(io::Error::other)?,
        }
        assert_eq!(old_decisions, [2]);

        let mut new_decisions = Vec::new();
        let mut limit_to_one = |scope: ExecutionScope<'_>| {
            new_decisions.push(scope.documents.len());
            if scope.documents.len() > 1 {
                Err(ExecutionDenied::new(
                    "test.document_limit",
                    "One Document is permitted",
                    None,
                ))
            } else {
                Ok(())
            }
        };
        if during_write {
            supervisor
                .check_execution(&mut limit_to_one)
                .map_err(io::Error::other)?;
        }
        let _ = release.send(());
        if during_write {
            assert!(matches!(
                supervisor.next_event(&mut limit_to_one).await,
                RunnerManagementEvent::Failure(RunnerManagementSupervisorError::ExecutionDenied(error))
                    if error.code() == "test.document_limit"
            ));
            assert!(matches!(put.await, Err(RunnerUnavailable)));
            assert_eq!(new_decisions, [1, 2]);
        } else {
            let RunnerManagementEvent::CommitReady(commit) = supervisor
                .next_event(|_| {
                    Err(ExecutionDenied::new(
                        "test.unexpected_call",
                        "No policy call is necessary",
                        None,
                    ))
                })
                .await
            else {
                return Err(io::Error::other("Document commit did not complete"));
            };
            commit.publish_after(|directives| {
                assert_eq!(directives.len(), 1);
                Ok::<_, io::Error>(())
            })?;
            assert!(matches!(put.await, Ok(Ok(_))));
            assert!(supervisor.check_execution(&mut limit_to_one).is_err());
            assert_eq!(new_decisions, [2]);
        }
        assert!(!supervisor.recheck_after_mutation);
        supervisor.shutdown().await.map_err(io::Error::other)?;
        let saved = fs::read_dir(directory.path().join("tenon-documents"))?
            .map(|entry| fs::read(entry?.path()))
            .collect::<io::Result<Vec<_>>>()?;
        assert_eq!(saved.len(), 2);
        assert!(saved.iter().any(|bytes| bytes == source.as_bytes()));
    }
    Ok(())
}
