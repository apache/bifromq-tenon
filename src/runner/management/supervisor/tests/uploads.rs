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

//! Upload completion, client lifetime, Store handoff, and fatal spool failures.

use super::programs::{hold_mutation_before_filesystem_work, program_supervisor};
use super::*;
use crate::payload_contract::PluginInterface;
use crate::runner::management::RunnerPluginUpload;
use crate::runner::plugin::package::tests::valid_program_package;
use crate::runner::plugin::store::PluginProgramStore;
use bytes::Bytes;
use std::fs;
use std::future::{Future as _, poll_fn};
use std::task::Poll;

#[tokio::test(flavor = "current_thread")]
async fn cancelling_before_end_rejects_even_a_complete_gzip_archive() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut supervisor, management) = program_supervisor(directory.path())?;
    let (mut upload, release) = hold_program_upload(&mut supervisor)?;
    upload
        .write(Bytes::from(valid_program_package(
            PluginInterface::SourceAndSink,
        )?))
        .await
        .map_err(|_| io::Error::other("Upload channel closed"))?;
    drop(upload);
    let (response, query) = oneshot::channel();
    management
        .queries
        .send(RunnerManagementQuery::ListPrograms {
            interface: None,
            response,
        })
        .await
        .map_err(|_| io::Error::other("Query was not queued"))?;
    let _ = release.send(());
    let RunnerManagementEvent::CommitReady(commit) = supervisor.next_event(|_| Ok(())).await else {
        return Err(io::Error::other(
            "Client cancellation became a fatal Store error",
        ));
    };
    commit.publish_after(|directives| {
        assert!(directives.is_empty());
        Ok::<_, io::Error>(())
    })?;
    assert!(supervisor.state.owns_program_store());
    tokio::select! {
        _ = supervisor.next_event(|_| Ok(())) => return Err(io::Error::other("Query created a mutation event")),
        result = query => {
            let entries = result.map_err(io::Error::other)?
                .map_err(|_| io::Error::other("Cancelled upload lost the Store"))?;
            assert_eq!(entries.len(), 2);
        }
    }
    assert_eq!(
        directory
            .path()
            .join("plugins/programs")
            .read_dir()?
            .count(),
        2
    );
    supervisor.shutdown().await.map_err(io::Error::other)?;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancellation_after_end_does_not_abandon_installation_during_shutdown() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut supervisor, management) = program_supervisor(directory.path())?;
    let (mut upload, release) = hold_program_upload(&mut supervisor)?;
    upload
        .write(Bytes::from(valid_program_package(
            PluginInterface::SourceAndSink,
        )?))
        .await
        .map_err(|_| io::Error::other("Upload channel closed"))?;
    {
        let finish = upload.finish();
        tokio::pin!(finish);
        poll_fn(|context| {
            assert!(finish.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    let (response, query) = oneshot::channel();
    management
        .queries
        .send(RunnerManagementQuery::ListPrograms {
            interface: None,
            response,
        })
        .await
        .map_err(|_| io::Error::other("Query was not queued"))?;
    {
        let shutdown = supervisor.shutdown();
        tokio::pin!(shutdown);
        tokio::select! {
            _ = &mut shutdown => return Err(io::Error::other("Shutdown abandoned the owned upload")),
            result = query => assert!(matches!(result, Ok(Err(RunnerUnavailable)))),
        }
        let _ = release.send(());
        shutdown.await.map_err(io::Error::other)?;
    }
    drop(supervisor);
    let recovered = PluginProgramStore::recover(
        directory.path().join("plugins/programs"),
        directory.path().join("plugins/programs"),
        std::sync::Arc::new(crate::runner::extensions::ByPass),
    )
    .map_err(io::Error::other)?;
    assert_eq!(recovered.programs().count(), 3);
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn spool_creation_failure_is_fatal_and_rejects_queued_work() -> io::Result<()> {
    let directory = tempfile::tempdir()?;
    let (mut supervisor, management) = program_supervisor(directory.path())?;
    let (upload, release) = hold_program_upload(&mut supervisor)?;
    let root = directory.path().join("plugins/programs");
    fs::rename(&root, directory.path().join("externally-moved-programs"))?;
    fs::write(&root, b"not a directory")?;
    let (response, query) = oneshot::channel();
    management
        .queries
        .send(RunnerManagementQuery::ListPrograms {
            interface: None,
            response,
        })
        .await
        .map_err(|_| io::Error::other("Query was not queued"))?;
    let queued_upload = management
        .begin_program_install()
        .await
        .map_err(|_| io::Error::other("Second upload was not queued"))?;
    let _ = release.send(());
    let RunnerManagementEvent::Failure(RunnerManagementSupervisorError::ProgramStore(error)) =
        supervisor.next_event(|_| Ok(())).await
    else {
        return Err(io::Error::other(
            "Spool failure did not terminate management",
        ));
    };
    assert!(
        matches!(error, PluginStoreError::FilesystemOperationFailed { path, source }
        if path == root && source.kind() == io::ErrorKind::NotADirectory)
    );
    assert!(upload.finish().await.is_err());
    supervisor.shutdown().await.map_err(io::Error::other)?;
    assert!(matches!(query.await, Ok(Err(RunnerUnavailable))));
    assert!(queued_upload.finish().await.is_err());
    assert!(management.begin_program_install().await.is_err());
    Ok(())
}

fn hold_program_upload(
    supervisor: &mut RunnerManagementSupervisor,
) -> io::Result<(RunnerPluginUpload, oneshot::Sender<()>)> {
    let (upload, package, response) = RunnerPluginUpload::channel();
    let job = supervisor
        .state
        .start_mutation(RunnerManagementMutation::InstallProgram { package, response })
        .ok_or_else(|| io::Error::other("Upload did not take the Store"))?;
    Ok((
        upload,
        hold_mutation_before_filesystem_work(supervisor, job),
    ))
}
