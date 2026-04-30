use sqlparser::ast::{self, UnaryOperator};

use crate::error::{Result, SqlError};
use crate::types::*;

pub(super) fn convert_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
    match op {
        ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
        ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
        ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
        ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
        ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
        ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
        ast::BinaryOperator::NotEq => Ok(BinaryOp::Ne),
        ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
        ast::BinaryOperator::GtEq => Ok(BinaryOp::Ge),
        ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
        ast::BinaryOperator::LtEq => Ok(BinaryOp::Le),
        ast::BinaryOperator::And => Ok(BinaryOp::And),
        ast::BinaryOperator::Or => Ok(BinaryOp::Or),
        ast::BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
        _ => Err(SqlError::Unsupported {
            detail: format!("binary operator: {op}"),
        }),
    }
}

pub(super) fn convert_unary_op(op: &UnaryOperator) -> Result<UnaryOp> {
    match op {
        UnaryOperator::Minus => Ok(UnaryOp::Neg),
        UnaryOperator::Not => Ok(UnaryOp::Not),
        _ => Err(SqlError::Unsupported {
            detail: format!("unary operator: {op}"),
        }),
    }
}
