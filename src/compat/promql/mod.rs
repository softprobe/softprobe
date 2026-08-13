//! PromQL subset: parse with promql-parser, lower unsupported AST, evaluate.

mod eval;
mod parse;

pub use eval::{eval_instant, eval_range, EvalResult, InstantSample, MatrixResult, VectorResult};
pub use parse::{parse_match_selector, parse_promql, ParsedSelector};
