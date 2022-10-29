use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use cyfs_base::*;
use cyfs_lib::{NDNAPILevel, NDNOutputRequestCommon, SharedCyfsStack, TransCreateTaskOutputRequest, TransGetTaskStateOutputRequest, TransTaskOutputRequest, TransTaskState};
use cyfs_task_manager::{Runnable, RunnableTask, Task, TaskCategory, TaskFactory, TaskId, TaskStatus, TaskStore, TaskType};
use cyfs_dsg_client::{DsgContractObject, DsgContractState, DsgContractStateObject, DsgContractStateObjectRef, DsgIgnoreWitness, RecoveryState};
use crate::DsgService;
use sha2::{Digest};

pub const RECOVERY_TASK: TaskType = TaskType(1);
pub const RECOVERY_TASK_CATEGORY: TaskCategory = TaskCategory(1);

pub struct RecoveryTask {
    stack: Arc<SharedCyfsStack>,
    service: DsgService,
    task_id: TaskId,
    contract_id: ObjectId,
    latest_state_id: ObjectId,
    target_id: ObjectId,
    target_dec_id: ObjectId,
    state: Mutex<RecoveryState>,
}

impl RecoveryTask {
    pub fn new(service: DsgService, contract_id: ObjectId, latest_state_id: ObjectId, target_id: ObjectId, target_dec_id: ObjectId) -> Self {
        let mut sha256 = sha2::Sha256::new();
        sha256.update(RECOVERY_TASK.0.to_le_bytes());
        sha256.update(RECOVERY_TASK_CATEGORY.0.to_le_bytes());
        sha256.update(contract_id.as_slice());
        let task_id = TaskId::from(sha256.finalize().as_ref());
        let stack = Arc::new(service.stack().clone());
        Self {
            stack,
            service,
            task_id,
            contract_id,
            latest_state_id,
            target_id,
            target_dec_id,
            state: Mutex::new(RecoveryState::Init)
        }
    }

    async fn run_proc(&self) -> BuckyResult<()> {
        let _: DsgContractObject<DsgIgnoreWitness> = self.service.get_object_from_device(self.target_id.clone(), self.contract_id.clone()).await?;
        let mut state: DsgContractStateObject = self.service.get_object_from_device(self.target_id.clone(), self.latest_state_id.clone()).await?;
        let latest_state = state.clone();
        let mut all_states = vec![self.latest_state_id.clone()];

        loop {
            let prev = self.download_from_state(&state).await?;
            if prev.is_none() {
                break;
            }

            let state_id = prev.as_ref().unwrap().clone();
            all_states.push(state_id);
            state = self.service.get_object_from_device(self.target_id.clone(), prev.unwrap()).await?;
        }
        self.service.recovery_contract_meta(&self.contract_id, &latest_state, all_states).await?;

        Ok(())
    }

    async fn download_from_state(&self, state: &DsgContractStateObject) -> BuckyResult<Option<ObjectId>> {
        let state_ref = DsgContractStateObjectRef::from(state);
        if let DsgContractState::DataSourceChanged(change_info) = state_ref.state() {
            let miner_id = self.service.get_miner_device_id(&self.target_id).await?;
            self.download(change_info.chunks.clone(), vec![DeviceId::try_from(miner_id)?]).await?;
            Ok(change_info.prev_change.clone())
        } else {
            unreachable!()
        }
    }

    async fn download(&self, chunk_list: Vec<ChunkId>, source_list: Vec<DeviceId>) -> BuckyResult<()> {
        let chunk_bundle = ChunkBundle::new(chunk_list, ChunkBundleHashMethod::Serial);
        let owner_id = self.stack.local_device().desc().owner().clone().unwrap();
        let file = File::new(owner_id, chunk_bundle.len(), chunk_bundle.calc_hash_value(), ChunkList::ChunkInBundle(chunk_bundle)).no_create_time().build();
        let file_id = file.desc().object_id();
        self.service.put_object_to_noc(file_id.clone(), &file).await?;

        let task_id = self.stack.trans().create_task(&TransCreateTaskOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            object_id: file_id,
            local_path: PathBuf::new(),
            device_list: source_list,
            context_id: None,
            auto_start: true
        }).await?.task_id;

        loop {
            let state = self.stack.trans().get_task_state(&TransGetTaskStateOutputRequest {
                common: NDNOutputRequestCommon {
                    req_path: None,
                    dec_id: None,
                    level: NDNAPILevel::NDC,
                    target: None,
                    referer_object: vec![],
                    flags: 0
                },
                task_id: task_id.clone()
            }).await?;

            match state {
                TransTaskState::Pending => {

                }
                TransTaskState::Downloading(_) => {

                }
                TransTaskState::Paused | TransTaskState::Canceled => {
                    let msg = format!("download {} task abnormal exit.", file_id.to_string());
                    log::error!("{}", msg.as_str());
                    return Err(BuckyError::new(BuckyErrorCode::Failed, msg))
                }
                TransTaskState::Finished(_) => {
                    break;
                }
                TransTaskState::Err(err) => {
                    let msg = format!("download {} failed.{}", file_id.to_string(), err);
                    log::error!("{}", msg.as_str());
                    return Err(BuckyError::new(err, msg))
                }
            }
            async_std::task::sleep(Duration::from_secs(1)).await;
        }
        self.stack.trans().delete_task(&TransTaskOutputRequest {
            common: NDNOutputRequestCommon {
                req_path: None,
                dec_id: None,
                level: NDNAPILevel::NDC,
                target: None,
                referer_object: vec![],
                flags: 0
            },
            task_id
        }).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Runnable for RecoveryTask {
    fn get_task_id(&self) -> TaskId {
        self.task_id.clone()
    }

    fn get_task_type(&self) -> TaskType {
        RECOVERY_TASK
    }

    fn get_task_category(&self) -> TaskCategory {
        RECOVERY_TASK_CATEGORY
    }

    async fn set_task_store(&mut self, _task_store: Arc<dyn TaskStore>) {
    }

    async fn run(&self) -> BuckyResult<()> {
        *self.state.lock().unwrap() = RecoveryState::Recovering;
        if let Err(e) = self.run_proc().await {
            *self.state.lock().unwrap() = RecoveryState::Err(format!("{}", e));
            Err(e)
        } else {
            *self.state.lock().unwrap() = RecoveryState::Finished;
            Ok(())
        }
    }

    async fn get_task_detail_status(&self) -> BuckyResult<Vec<u8>> {
        Ok(self.state.lock().unwrap().to_vec()?)
    }
}

#[derive(RawEncode, RawDecode, Clone)]
pub struct RecoveryTaskParam {
    pub contract_id: ObjectId,
    pub latest_state_id: ObjectId,
    pub target_id: ObjectId,
    pub target_dec_id: ObjectId,
}

pub struct RecoveryTaskFactory {
    service: DsgService,
}

impl RecoveryTaskFactory {
    pub fn new(service: DsgService) -> Self {
        Self {
            service
        }
    }

}

#[async_trait::async_trait]
impl TaskFactory for RecoveryTaskFactory {
    fn get_task_type(&self) -> TaskType {
        RECOVERY_TASK
    }

    async fn create(&self, params: &[u8]) -> BuckyResult<Box<dyn Task>> {
        let params = RecoveryTaskParam::clone_from_slice(params)?;
        Ok(Box::new(RunnableTask::new(RecoveryTask::new(
            self.service.clone(),
            params.contract_id,
            params.latest_state_id,
            params.target_id,
            params.target_dec_id))))
    }

    async fn restore(&self, _task_status: TaskStatus, params: &[u8], _data: &[u8]) -> BuckyResult<Box<dyn Task>> {
        let params = RecoveryTaskParam::clone_from_slice(params)?;
        Ok(Box::new(RunnableTask::new(RecoveryTask::new(
            self.service.clone(),
            params.contract_id,
            params.latest_state_id,
            params.target_id,
            params.target_dec_id))))
    }
}
