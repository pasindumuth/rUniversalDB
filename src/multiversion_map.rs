use crate::model::common::Timestamp;
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;
use std::hash::Hash;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MVM<K: Eq + Ord + Clone, V> {
  pub map: BTreeMap<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>,
}

impl<K, V> MVM<K, V>
where
  K: Eq + Ord + Clone,
  V: Clone,
{
  pub fn new() -> MVM<K, V> {
    MVM { map: BTreeMap::new() }
  }

  pub fn init(init_vals: BTreeMap<K, V>) -> MVM<K, V> {
    let mut map = BTreeMap::<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>::new();
    for (key, value) in init_vals {
      map.insert(key, (0, vec![(0, Some(value))]));
    }
    MVM { map }
  }

  /// Performs an MVMWrite to the MVM. The user *must* be sure that `timestamp` is beyond the
  /// `lat` of the key, otherwise we `assert`. They can verify this by doing static and weak reads.
  pub fn write(&mut self, key: &K, value: Option<V>, timestamp: Timestamp) {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      assert!(*lat < timestamp);
      *lat = timestamp;
      versions.push((timestamp, value));
    } else {
      // Recall that mathematically, every key imaginable is in the MVM with no versions
      // and a LAT of 0. Thus, we cannot do an MVM write to a timestamp of 0.
      assert!(0 < timestamp);
      self.map.insert(key.clone(), (timestamp, vec![(timestamp, value)]));
    }
  }

  pub fn read(&mut self, key: &K, timestamp: Timestamp) -> Option<V> {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      *lat = max(*lat, timestamp);
      find_version(versions, timestamp).cloned()
    } else {
      self.map.insert(key.clone(), (timestamp, vec![]));
      None
    }
  }

  pub fn update_lat(&mut self, key: &K, timestamp: Timestamp) {
    if let Some((lat, _)) = self.map.get_mut(key) {
      *lat = max(*lat, timestamp);
    } else {
      self.map.insert(key.clone(), (timestamp, vec![]));
    }
  }

  /// Reads the version prior to the timestamp. Asserts that the `lat` is high enough.
  pub fn strong_static_read(&self, key: &K, timestamp: Timestamp) -> Option<&V> {
    if let Some((lat, versions)) = self.map.get(key) {
      assert!(&timestamp <= lat);
      find_version(versions, timestamp)
    } else {
      None
    }
  }

  /// Get the value that would be read if we did a `read` at the `lat`.
  pub fn get_last_version(&self, key: &K) -> Option<&V> {
    if let Some((_, versions)) = self.map.get(key) {
      if let Some((_, val)) = versions.iter().last() {
        val.as_ref()
      } else {
        None
      }
    } else {
      None
    }
  }

  /// Get the latest version of the `key` that was non-`None`.
  pub fn get_last_present_version(&self, key: &K) -> Option<&V> {
    if let Some((_, versions)) = self.map.get(key) {
      for (_, val) in versions.iter().rev() {
        if val.is_some() {
          return val.as_ref();
        }
      }
    }
    None
  }

  /// Reads the version prior to the timestamp.
  /// NOTE: This does not mutate the `lat` if the read happens with a future timestamp.
  pub fn static_read(&self, key: &K, timestamp: Timestamp) -> Option<&V> {
    if let Some((_, versions)) = self.map.get(key) {
      find_version(versions, timestamp)
    } else {
      None
    }
  }

  /// Returns the values for all keys that are present at the given
  /// `timestamp`. This is done statically, so no lats are updated.
  pub fn static_snapshot_read(&self, timestamp: Timestamp) -> BTreeMap<K, V> {
    let mut snapshot = BTreeMap::new();
    for (key, (_, versions)) in &self.map {
      if let Some(value) = find_version(versions, timestamp) {
        snapshot.insert(key.clone(), value.clone());
      }
    }
    return snapshot;
  }

  /// Recall that abstractly, all keys are mapped to `(0, [])`
  pub fn get_lat(&self, key: &K) -> Timestamp {
    if let Some((lat, _)) = self.map.get(key) {
      *lat
    } else {
      0
    }
  }

  /// Get the highest LAT of any key-value pair in the MVM.
  pub fn get_latest_lat(&self) -> Timestamp {
    let mut latest_lat = 0;
    for (_, (lat, _)) in &self.map {
      latest_lat = max(latest_lat, *lat);
    }
    latest_lat
  }
}

pub fn find_version<V>(versions: &Vec<(Timestamp, Option<V>)>, timestamp: Timestamp) -> Option<&V> {
  for (t, value) in versions.iter().rev() {
    if *t <= timestamp {
      return value.as_ref();
    }
  }
  return None;
}

#[cfg(test)]
mod tests {
  use crate::multiversion_map::MVM;

  #[test]
  fn single_key_test() {
    let mut mvm = MVM::new();
    let k = String::from("k");
    let v1 = String::from("v1");
    let v2 = String::from("v2");
    let v3 = String::from("v3");
    assert_eq!(mvm.read(&k, 1), None);
    mvm.write(&k, Some(v1.clone()), 2);
    mvm.write(&k, Some(v2.clone()), 4);
    assert_eq!(mvm.read(&k, 3), Some(v1));
    assert_eq!(mvm.read(&k, 5), Some(v2));
    mvm.write(&k, Some(v3.clone()), 6);
    assert_eq!(mvm.read(&k, 6), Some(v3));
    mvm.write(&k, None, 7);
    assert_eq!(mvm.read(&k, 7), None);
  }
}
