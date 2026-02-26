use crate::task::{Task, TaskId, TaskState};
use sdb_common::{Result, SdbError};
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

    /// Submit a new task. Returns its ID.
    pub fn submit(&mut self, name: impl Into<String>) -> TaskId {
        let id = self.next_id;
        self.next_id += 1;
        self.tasks.insert(id, Task::new(id, name));
        id
    }

    /// Transition a task from Pending to Running.
    pub fn start(&mut self, id: TaskId) -> Result<()> {
        let task = self.tasks.get_mut(&id).ok_or(SdbError::TaskNotFound)?;
        if task.state != TaskState::Pending {
            return Err(SdbError::InvalidArg);
        }
        task.state = TaskState::Running;
        Ok(())
    }

    /// Mark a running task as completed.
    pub fn complete(&mut self, id: TaskId) -> Result<()> {
        let task = self.tasks.get_mut(&id).ok_or(SdbError::TaskNotFound)?;
        if task.state != TaskState::Running {
            return Err(SdbError::InvalidArg);
        }
        task.state = TaskState::Completed;
        Ok(())
    }

    /// Mark a running task as failed.
    pub fn fail(&mut self, id: TaskId) -> Result<()> {
        let task = self.tasks.get_mut(&id).ok_or(SdbError::TaskNotFound)?;
        if task.state != TaskState::Running {
            return Err(SdbError::InvalidArg);
        }
        task.state = TaskState::Failed;
        Ok(())
    }

    /// Cancel a pending or running task.
    pub fn cancel(&mut self, id: TaskId) -> Result<()> {
        let task = self.tasks.get_mut(&id).ok_or(SdbError::TaskNotFound)?;
        match task.state {
            TaskState::Pending | TaskState::Running => {
                task.state = TaskState::Cancelled;
                Ok(())
            }
            _ => Err(SdbError::InvalidArg),
        }
    }

    pub fn get_state(&self, id: TaskId) -> Result<TaskState> {
        self.tasks
            .get(&id)
            .map(|t| t.state)
            .ok_or(SdbError::TaskNotFound)
    }

    pub fn get(&self, id: TaskId) -> Result<&Task> {
        self.tasks.get(&id).ok_or(SdbError::TaskNotFound)
    }

    pub fn list(&self) -> Vec<&Task> {
        let mut tasks: Vec<&Task> = self.tasks.values().collect();
        tasks.sort_by_key(|t| t.id);
        tasks
    }

    /// Return count of tasks in a given state.
    pub fn count_by_state(&self, state: TaskState) -> usize {
        self.tasks.values().filter(|t| t.state == state).count()
    }

    /// Remove completed, failed, and cancelled tasks.
    pub fn purge_finished(&mut self) -> usize {
        let before = self.tasks.len();
        self.tasks.retain(|_, t| matches!(t.state, TaskState::Pending | TaskState::Running));
        before - self.tasks.len()
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn submit_assigns_ids() {
        let mut s = Scheduler::new();
        let id1 = s.submit("task1");
        let id2 = s.submit("task2");
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
    }

    #[test]
    fn full_lifecycle() {
        let mut s = Scheduler::new();
        let id = s.submit("build_index");
        assert_eq!(s.get_state(id).unwrap(), TaskState::Pending);

        s.start(id).unwrap();
        assert_eq!(s.get_state(id).unwrap(), TaskState::Running);

        s.complete(id).unwrap();
        assert_eq!(s.get_state(id).unwrap(), TaskState::Completed);
    }

    #[test]
    fn fail_lifecycle() {
        let mut s = Scheduler::new();
        let id = s.submit("migrate");
        s.start(id).unwrap();
        s.fail(id).unwrap();
        assert_eq!(s.get_state(id).unwrap(), TaskState::Failed);
    }

    #[test]
    fn cancel_pending() {
        let mut s = Scheduler::new();
        let id = s.submit("split");
        s.cancel(id).unwrap();
        assert_eq!(s.get_state(id).unwrap(), TaskState::Cancelled);
    }

    #[test]
    fn cancel_running() {
        let mut s = Scheduler::new();
        let id = s.submit("reindex");
        s.start(id).unwrap();
        s.cancel(id).unwrap();
        assert_eq!(s.get_state(id).unwrap(), TaskState::Cancelled);
    }

    #[test]
    fn cannot_start_completed() {
        let mut s = Scheduler::new();
        let id = s.submit("t");
        s.start(id).unwrap();
        s.complete(id).unwrap();
        assert!(s.start(id).is_err());
    }

    #[test]
    fn cannot_cancel_completed() {
        let mut s = Scheduler::new();
        let id = s.submit("t");
        s.start(id).unwrap();
        s.complete(id).unwrap();
        assert!(s.cancel(id).is_err());
    }

    #[test]
    fn nonexistent_task() {
        let s = Scheduler::new();
        assert!(s.get_state(999).is_err());
    }

    #[test]
    fn list_sorted_by_id() {
        let mut s = Scheduler::new();
        s.submit("a");
        s.submit("b");
        s.submit("c");
        let tasks = s.list();
        assert_eq!(tasks.len(), 3);
        assert!(tasks[0].id < tasks[1].id);
        assert!(tasks[1].id < tasks[2].id);
    }

    #[test]
    fn count_by_state() {
        let mut s = Scheduler::new();
        s.submit("a");
        s.submit("b");
        let id3 = s.submit("c");
        s.start(id3).unwrap();
        assert_eq!(s.count_by_state(TaskState::Pending), 2);
        assert_eq!(s.count_by_state(TaskState::Running), 1);
    }

    #[test]
    fn purge_finished() {
        let mut s = Scheduler::new();
        let id1 = s.submit("a");
        s.submit("b");
        s.start(id1).unwrap();
        s.complete(id1).unwrap();
        let purged = s.purge_finished();
        assert_eq!(purged, 1);
        assert_eq!(s.list().len(), 1);
    }
}
