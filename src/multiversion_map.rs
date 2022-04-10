use crate::common::Timestamp;
use crate::common::{mk_t, update_all_eids};
use serde::{Deserialize, Serialize};
use std::cmp::max;
use std::collections::BTreeMap;
use std::hash::Hash;

/// Here, `min_lat` is used to increase the LATs of all Keys in existance (which is an
/// infinite set). When it is incremeted, the LATs of every key that is present in `map`
/// are updated as well so that they are always >= to `min_lat`.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct MVM<K: Eq + Ord + Clone, V> {
  min_lat: Timestamp,
  map: BTreeMap<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>,
}

impl<K, V> MVM<K, V>
where
  K: Eq + Ord + Clone,
  V: Clone,
{
  pub fn new() -> MVM<K, V> {
    MVM { min_lat: mk_t(0), map: BTreeMap::new() }
  }

  pub fn init(init_vals: BTreeMap<K, V>) -> MVM<K, V> {
    let mut map = BTreeMap::<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>::new();
    for (key, value) in init_vals {
      map.insert(key, (mk_t(0), vec![(mk_t(0), Some(value))]));
    }
    MVM { min_lat: mk_t(0), map }
  }

  /// Performs an MVMWrite to the MVM. The user *must* be sure that `timestamp` is beyond the
  /// `lat` of the key, otherwise we `assert`. They can verify this by doing static and weak reads.
  pub fn write(&mut self, key: &K, value: Option<V>, timestamp: Timestamp) {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      assert!(*lat < timestamp);
      *lat = timestamp.clone();
      versions.push((timestamp, value));
    } else {
      // Here, the `key` has a LAT of `min_lat` and contains no versions.
      assert!(self.min_lat < timestamp);
      self.map.insert(key.clone(), (timestamp.clone(), vec![(timestamp, value)]));
    }
  }

  pub fn read(&mut self, key: &K, timestamp: &Timestamp) -> Option<V> {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      *lat = max(lat.clone(), timestamp.clone());
      find_prior_value(versions, timestamp).cloned()
    } else {
      if timestamp > &self.min_lat {
        self.map.insert(key.clone(), (timestamp.clone(), vec![]));
      }

      None
    }
  }

  pub fn update_lat(&mut self, key: &K, timestamp: Timestamp) {
    if let Some((lat, _)) = self.map.get_mut(key) {
      *lat = max(lat.clone(), timestamp);
    } else if timestamp > self.min_lat {
      self.map.insert(key.clone(), (timestamp, vec![]));
    }
  }

  pub fn update_all_lats(&mut self, timestamp: Timestamp) {
    if timestamp > self.min_lat {
      for (_, (lat, _)) in &mut self.map {
        *lat = max(lat.clone(), timestamp.clone())
      }
      self.min_lat = timestamp;
    }
  }

  /// Reads the version prior to the timestamp. This function asserts that the `lat` of
  /// the `key` is `>= timestamp`. Recall that all keys in existance implicitly at
  /// least have a `lat` of 0. Thus, the return value of this function is idempotent.
  pub fn strong_static_read(&self, key: &K, timestamp: &Timestamp) -> Option<&V> {
    if let Some((lat, versions)) = self.map.get(key) {
      assert!(timestamp <= lat);
      find_prior_value(versions, timestamp)
    } else {
      assert!(timestamp <= &self.min_lat);
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

  /// Reads the prior value at the timestamp. This does not mutate the `lat` if the read
  /// happens with a future timestamp. Thus, the values read are not idempotent.
  pub fn static_read(&self, key: &K, timestamp: &Timestamp) -> Option<&V> {
    let (_, value) = self.static_read_version(key, timestamp)?;
    value.as_ref()
  }

  /// Reads the prior version at the timestamp. This does not mutate the `lat` if the read
  /// happens with a future timestamp. Thus, the values read are not idempotent.
  pub fn static_read_version(
    &self,
    key: &K,
    timestamp: &Timestamp,
  ) -> Option<&(Timestamp, Option<V>)> {
    if let Some((_, versions)) = self.map.get(key) {
      find_prior_version(versions, timestamp)
    } else {
      None
    }
  }

  /// Returns the values for all keys that are present at the given
  /// `timestamp`. This is done statically, so no lats are updated.
  pub fn static_snapshot_read(&self, timestamp: &Timestamp) -> BTreeMap<K, V> {
    let mut snapshot = BTreeMap::new();
    for (key, (_, versions)) in &self.map {
      if let Some(value) = find_prior_value(versions, timestamp) {
        snapshot.insert(key.clone(), value.clone());
      }
    }
    return snapshot;
  }

  /// Recall that abstractly, all keys are mapped to `(0, [])`
  pub fn get_lat(&self, key: &K) -> Timestamp {
    if let Some((lat, _)) = self.map.get(key) {
      lat.clone()
    } else {
      self.min_lat.clone()
    }
  }

  /// Get the smallest LAT among all keys. There certainly exists a key with a LAT of
  /// `min_lat`, since there are infinite keys. Thus, we simply return `min_lat`.
  pub fn get_min_lat(&self) -> Timestamp {
    self.min_lat.clone()
  }

  /// Get the highest LAT of any key-value pair in the MVM.
  pub fn get_latest_lat(&self) -> Timestamp {
    let mut latest_lat = self.min_lat.clone();
    for (_, (lat, _)) in &self.map {
      latest_lat = max(latest_lat, lat.clone());
    }
    latest_lat
  }
}

fn find_prior_value<'a, V>(
  versions: &'a Vec<(Timestamp, Option<V>)>,
  timestamp: &Timestamp,
) -> Option<&'a V> {
  let (_, value) = find_prior_version(versions, timestamp)?;
  value.as_ref()
}

fn find_prior_version<'a, V>(
  versions: &'a Vec<(Timestamp, Option<V>)>,
  timestamp: &Timestamp,
) -> Option<&'a (Timestamp, Option<V>)> {
  for version in versions.iter().rev() {
    let (t, _) = version;
    if t <= timestamp {
      return Some(version);
    }
  }
  return None;
}

#[cfg(test)]
mod tests {
  use crate::common::mk_t;
  use crate::common::Timestamp;
  use crate::multiversion_map::MVM;

  #[test]
  fn single_key_test() {
    let mut mvm = MVM::new();
    let k = String::from("k");
    let v1 = String::from("v1");
    let v2 = String::from("v2");
    let v3 = String::from("v3");
    assert_eq!(mvm.read(&k, &mk_t(1)), None);
    mvm.write(&k, Some(v1.clone()), mk_t(2));
    mvm.write(&k, Some(v2.clone()), mk_t(4));
    assert_eq!(mvm.read(&k, &mk_t(3)), Some(v1));
    assert_eq!(mvm.read(&k, &mk_t(5)), Some(v2));
    mvm.write(&k, Some(v3.clone()), mk_t(6));
    assert_eq!(mvm.read(&k, &mk_t(6)), Some(v3));
    mvm.write(&k, None, mk_t(7));
    assert_eq!(mvm.read(&k, &mk_t(7)), None);
  }
}
