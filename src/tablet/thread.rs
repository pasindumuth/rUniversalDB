use crate::model::common::EndpointId;
use crate::model::message::TabletMessage;
use crate::tablet::tablet::TabletState;
use rand::RngCore;

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

pub fn start_tablet_thread(
    rand_gen: Box<dyn RngCore>,
    _receiver: Receiver<TabletMessage>,
    _net_conn_map: Arc<Mutex<HashMap<EndpointId, Sender<Vec<u8>>>>>,
) {
    let _state = TabletState { rand_gen };
}
