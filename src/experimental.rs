use crate::sql_ast::proc;
use std::mem;
use std::ops::Deref;

// In this file, we store experimental implementations for things.

// Implementations

struct SubqueryIter<'a> {
  expr: &'a proc::ValExpr,
  parent: Option<Box<SubqueryIter<'a>>>,
}

// impl proc::ValExpr {
//   fn subquery_iter(&self) -> SubqueryIter<'_> {
//     SubqueryIter { expr: self, parent: None }
//   }
// }

impl<'a> Iterator for SubqueryIter<'a> {
  type Item = &'a proc::GRQuery;
  // The property here is that we should return the GRQuerys in `expr` in this
  // node, then the parent node, then its' parent, and so-on.
  fn next(&mut self) -> Option<Self::Item> {
    match self.expr {
      proc::ValExpr::ColumnRef(_) => {
        if let Some(parent) = self.parent.take() {
          *self = *parent;
          self.next()
        } else {
          None
        }
      }
      proc::ValExpr::UnaryExpr { expr, .. } => {
        self.expr = expr.deref();
        self.next()
      }
      proc::ValExpr::BinaryExpr { left, right, .. } => {
        self.parent =
          Some(Box::new(SubqueryIter { expr: right, parent: mem::take(&mut self.parent) }));
        self.expr = left;
        self.next()
      }
      proc::ValExpr::Value { .. } => {
        if let Some(parent) = self.parent.take() {
          *self = *parent;
          self.next()
        } else {
          None
        }
      }
      proc::ValExpr::Subquery { query } => {
        if let Some(parent) = self.parent.take() {
          *self = *parent;
          Some(query)
        } else {
          None
        }
      }
    }
  }
}
