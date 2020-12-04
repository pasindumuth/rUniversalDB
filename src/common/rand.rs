use rand::RngCore;
use serde::export::fmt::{Debug, Result};
use serde::export::Formatter;

pub struct RandGen {
    pub rng: Box<dyn RngCore>,
}

impl Debug for RandGen {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_tuple("rand_gen").finish()
    }
}
