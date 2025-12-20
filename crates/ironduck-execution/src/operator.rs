//! Physical operators for query execution

use super::chunk::DataChunk;
use ironduck_common::Result;

/// Result of getting data from a source
pub enum SourceResult {
    /// Source has more data
    HaveData,
    /// Source is finished
    Finished,
    /// Source is blocked (async)
    Blocked,
}

/// Result of sinking data
pub enum SinkResult {
    /// Need more data
    NeedData,
    /// Sink is finished
    Finished,
    /// Sink is blocked (async)
    Blocked,
}

/// Result of operator execution
pub enum OperatorResult {
    /// Need more input
    NeedInput,
    /// Have output ready
    HaveOutput,
    /// Operator is finished
    Finished,
    /// Operator is blocked (async)
    Blocked,
}

/// A source operator that produces data
pub trait Source: Send {
    /// Get the next batch of data
    fn get_data(&mut self, chunk: &mut DataChunk) -> Result<SourceResult>;

    /// Reset the source for rescanning
    fn reset(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A sink operator that consumes data
pub trait Sink: Send {
    /// Consume a batch of data
    fn sink(&mut self, chunk: &DataChunk) -> Result<SinkResult>;

    /// Called when all data has been sunk
    fn finalize(&mut self) -> Result<()>;
}

/// An operator that transforms data
pub trait Operator: Send {
    /// Execute on input, produce output
    fn execute(&mut self, input: &DataChunk, output: &mut DataChunk) -> Result<OperatorResult>;

    /// Called when there's no more input
    fn finalize(&mut self, _output: &mut DataChunk) -> Result<OperatorResult> {
        Ok(OperatorResult::Finished)
    }
}
