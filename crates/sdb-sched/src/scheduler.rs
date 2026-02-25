use sdb_common::Result;
use crate::task::{Task, TaskId, TaskState};
use std::collections::HashMap;

/// Task scheduler — manages background tasks.
pub struct Scheduler {
    tasks: HashMap<TaskId, Task>,
    next_id: TaskId,
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            tasks: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn submit(&mut self, name: impl Into<String>) -> TaskId {
        let id = self.next_id;
        self.next_id += 1;
        self.tasks.insert(id, Task::new(id, name));
        id
    }

    pub fn cancel(&mut self, id: TaskId) -> Result<()> {
        self.tasks
            .get_mut(&id)
            .map(|t| t.state = TaskState::Cancelled)
            .ok_or(sdb_common::SdbError::TaskNotFound)
    }

    pub fn get_state(&self, id: TaskId) -> Result<TaskState> {
        self.tasks
            .get(&id)
            .map(|t| t.state)
            .ok_or(sdb_common::SdbError::TaskNotFound)
    }

    pub fn list(&self) -> Vec<&Task> {
        self.tasks.values().collect()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}
