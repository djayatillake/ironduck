//! Pipeline execution

use super::chunk::DataChunk;
use super::operator::{Operator, OperatorResult, Sink, SinkResult, Source, SourceResult};
use ironduck_common::Result;

/// A pipeline of operators
pub struct Pipeline {
    /// The data source
    source: Box<dyn Source>,
    /// Chain of operators
    operators: Vec<Box<dyn Operator>>,
    /// The final sink
    sink: Box<dyn Sink>,
}

impl Pipeline {
    pub fn new(
        source: Box<dyn Source>,
        operators: Vec<Box<dyn Operator>>,
        sink: Box<dyn Sink>,
    ) -> Self {
        Pipeline {
            source,
            operators,
            sink,
        }
    }

    /// Execute the pipeline to completion
    pub fn execute(&mut self) -> Result<()> {
        let mut input_chunk = DataChunk::empty();
        let mut output_chunk = DataChunk::empty();

        loop {
            // Get data from source
            match self.source.get_data(&mut input_chunk)? {
                SourceResult::Finished => break,
                SourceResult::Blocked => {
                    // In async version, would yield here
                    continue;
                }
                SourceResult::HaveData => {}
            }

            // Process through operator chain
            let mut current_input = &input_chunk;
            for op in &mut self.operators {
                match op.execute(current_input, &mut output_chunk)? {
                    OperatorResult::HaveOutput => {
                        std::mem::swap(&mut input_chunk, &mut output_chunk);
                        current_input = &input_chunk;
                    }
                    OperatorResult::Finished => break,
                    OperatorResult::NeedInput => continue,
                    OperatorResult::Blocked => continue,
                }
            }

            // Sink the results
            match self.sink.sink(&input_chunk)? {
                SinkResult::Finished => break,
                SinkResult::NeedData => {}
                SinkResult::Blocked => continue,
            }

            input_chunk.reset();
        }

        // Finalize operators
        for op in &mut self.operators {
            loop {
                match op.finalize(&mut output_chunk)? {
                    OperatorResult::HaveOutput => {
                        self.sink.sink(&output_chunk)?;
                    }
                    OperatorResult::Finished => break,
                    _ => continue,
                }
            }
        }

        // Finalize sink
        self.sink.finalize()?;

        Ok(())
    }
}
