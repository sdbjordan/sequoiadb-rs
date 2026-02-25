/// Unique task identifier.
pub type TaskId = u64;

/// Task execution state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

/// A scheduled task (e.g., background index build, split, data migration).
pub struct Task {
    pub id: TaskId,
    pub name: String,
    pub state: TaskState,
}

impl Task {
    pub fn new(id: TaskId, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            state: TaskState::Pending,
        }
    }
}
