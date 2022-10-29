use std::collections::HashMap;
use std::sync::Mutex;
use cyfs_base::{BuckyErrorCode, BuckyResult, DeviceId, ObjectId};
use cyfs_task_manager::{DecInfo, TaskCategory, TaskId, TaskManagerStore, TaskStatus, TaskStore, TaskType};
use cyfs_dsg_client::cyfs_err;

struct TaskInfo {
    pub task_id: TaskId,
    pub category: TaskCategory,
    pub task_type: TaskType,
    pub task_status: TaskStatus,
    pub dec_list: Vec<DecInfo>,
    pub task_params: Vec<u8>,
    pub task_data: Vec<u8>,
}

pub struct MemoryTaskStore {
    tasks: Mutex<HashMap<TaskId, TaskInfo>>
}

impl MemoryTaskStore {
    pub fn new() -> Self {
        Self {
            tasks: Mutex::new(HashMap::new())
        }
    }
}

#[async_trait::async_trait]
impl TaskManagerStore for MemoryTaskStore {
    async fn add_task(&self, task_id: &TaskId, category: TaskCategory, task_type: TaskType, task_status: TaskStatus, dec_list: Vec<DecInfo>, task_params: Vec<u8>) -> BuckyResult<()> {
        let task_info = TaskInfo {
            task_id: task_id.clone(),
            category,
            task_type,
            task_status,
            dec_list,
            task_params,
            task_data: vec![]
        };
        let mut tasks = self.tasks.lock().unwrap();
        tasks.insert(task_id.clone(), task_info);
        Ok(())
    }

    async fn get_task(&self, task_id: &TaskId) -> BuckyResult<(TaskCategory, TaskType, TaskStatus, Vec<u8>, Vec<u8>)> {
        let tasks = self.tasks.lock().unwrap();
        match tasks.get(task_id) {
            Some(info) => {
                Ok((info.category, info.task_type, info.task_status, info.task_params.clone(), info.task_data.clone()))
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }

    async fn get_tasks_by_status(&self, _status: TaskStatus) -> BuckyResult<Vec<(TaskId, TaskType, Vec<u8>, Vec<u8>)>> {
        todo!()
    }

    async fn get_tasks_by_category(&self, _category: TaskCategory) -> BuckyResult<Vec<(TaskId, TaskType, TaskStatus, Vec<u8>, Vec<u8>)>> {
        todo!()
    }

    async fn get_tasks_by_task_id(&self, _task_id_list: &[TaskId]) -> BuckyResult<Vec<(TaskId, TaskType, TaskStatus, Vec<u8>, Vec<u8>)>> {
        todo!()
    }

    async fn get_tasks(&self, _source: &DeviceId, _dec_id: &ObjectId, _category: TaskCategory, _task_status: TaskStatus, _range: Option<(u64, u32)>) -> BuckyResult<Vec<(TaskId, TaskType, TaskStatus, Vec<u8>, Vec<u8>)>> {
        todo!()
    }

    async fn get_dec_list(&self, task_id: &TaskId) -> BuckyResult<Vec<DecInfo>> {
        let tasks = self.tasks.lock().unwrap();
        match tasks.get(task_id) {
            Some(info) => {
                Ok(info.dec_list.clone())
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }

    async fn add_dec_info(&self, task_id: &TaskId, _category: TaskCategory, task_status: TaskStatus, dec_info: &DecInfo) -> BuckyResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get_mut(task_id) {
            Some(info) => {
                info.task_status = task_status;
                info.dec_list.push(dec_info.clone());
                Ok(())
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }

    async fn delete_dec_info(&self, task_id: &TaskId, dec_id: &ObjectId, source: &DeviceId) -> BuckyResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get_mut(task_id) {
            Some(info) => {
                for (index, dec_info) in info.dec_list.iter().enumerate() {
                    if dec_info.dec_id() == dec_id && dec_info.source() == source {
                        info.dec_list.remove(index);
                        break;
                    }
                }
                Ok(())
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }

    async fn delete_task(&self, task_id: &TaskId) -> BuckyResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        tasks.remove(task_id);
        Ok(())
    }
}

#[async_trait::async_trait]
impl TaskStore for MemoryTaskStore {
    async fn save_task(&self, task_id: &TaskId, task_status: TaskStatus, task_data: Vec<u8>) -> BuckyResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get_mut(task_id) {
            Some(info) => {
                info.task_status = task_status;
                info.task_data = task_data;
                Ok(())
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }

    async fn save_task_status(&self, task_id: &TaskId, task_status: TaskStatus) -> BuckyResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get_mut(task_id) {
            Some(info) => {
                info.task_status = task_status;
                Ok(())
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }

    async fn save_task_data(&self, task_id: &TaskId, task_data: Vec<u8>) -> BuckyResult<()> {
        let mut tasks = self.tasks.lock().unwrap();
        match tasks.get_mut(task_id) {
            Some(info) => {
                info.task_data = task_data;
                Ok(())
            },
            None => {
                Err(cyfs_err!(BuckyErrorCode::NotFound, "can't find task {}", task_id.to_string()))
            }
        }
    }
}
