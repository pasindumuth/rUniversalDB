use crate::model::common::Timestamp;
use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug, Clone)]
pub struct MVM<K, V> {
  pub map: HashMap<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>,
}

impl<K, V> MVM<K, V>
where
  K: Eq + Hash + Clone,
  V: Clone,
{
  pub fn new() -> MVM<K, V> {
    MVM { map: HashMap::new() }
  }

  pub fn init(init_map: HashMap<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>) -> MVM<K, V> {
    MVM { map: init_map }
  }

  pub fn write(&mut self, key: &K, value: Option<V>, timestamp: Timestamp) -> Result<(), String> {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      if timestamp <= *lat {
        Err(String::from("Timestamps must be >= current lat."))
      } else {
        *lat = timestamp;
        versions.push((timestamp, value));
        Ok(())
      }
    } else {
      self.map.insert(key.clone(), (timestamp, vec![(timestamp, value)]));
      Ok(())
    }
  }

  pub fn read(&mut self, key: &K, timestamp: Timestamp) -> Option<V> {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      *lat = max(*lat, timestamp);
      MVM::<K, V>::find_version(versions, timestamp)
    } else {
      self.map.insert(key.clone(), (timestamp, vec![]));
      None
    }
  }

  /// Reads the version prior to the timestamp. Asserts that the `lat` is high enough.
  pub fn strong_static_read(&self, key: &K, timestamp: Timestamp) -> Option<V> {
    if let Some((lat, versions)) = self.map.get(key) {
      assert!(&timestamp <= lat);
      MVM::<K, V>::find_version(versions, timestamp)
    } else {
      None
    }
  }

  /// Get the latest version at the `lat`
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

  /// Reads the version prior to the timestamp. This doesn't mutate
  /// the lat if the read happens with a future timestamp.
  pub fn static_read(&self, key: &K, timestamp: Timestamp) -> Option<V> {
    if let Some((_, versions)) = self.map.get(key) {
      MVM::<K, V>::find_version(versions, timestamp)
    } else {
      None
    }
  }

  /// Returns the values for all keys that are present at the given
  /// `timestamp`. This is done statically, so no lats are updated.
  pub fn static_snapshot_read(&self, timestamp: Timestamp) -> HashMap<K, V> {
    let mut snapshot = HashMap::new();
    for (key, (_, versions)) in &self.map {
      if let Some(value) = MVM::<K, V>::find_version(versions, timestamp) {
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
      Timestamp(0)
    }
  }

  fn find_version(versions: &Vec<(Timestamp, Option<V>)>, timestamp: Timestamp) -> Option<V> {
    for (t, value) in versions.iter().rev() {
      if *t <= timestamp {
        return value.clone();
      }
    }
    return None;
  }
}

#[cfg(test)]
mod tests {
  use crate::model::common::Timestamp;
  use crate::multiversion_map::MVM;

  #[test]
  fn single_key_test() {
    let mut mvm = MVM::new();
    let k = String::from("k");
    let v1 = String::from("v1");
    let v2 = String::from("v2");
    let v3 = String::from("v3");
    assert_eq!(mvm.read(&k, Timestamp(1)), None);
    assert!(mvm.write(&k, Some(v1.clone()), Timestamp(2)).is_ok());
    assert!(mvm.write(&k, Some(v2.clone()), Timestamp(4)).is_ok());
    assert_eq!(mvm.read(&k, Timestamp(3)), Some(v1));
    assert_eq!(mvm.read(&k, Timestamp(5)), Some(v2));
    assert!(mvm.write(&k, Some(v3.clone()), Timestamp(5)).is_err());
    assert!(mvm.write(&k, Some(v3.clone()), Timestamp(6)).is_ok());
    assert_eq!(mvm.read(&k, Timestamp(6)), Some(v3));
    assert!(mvm.write(&k, None, Timestamp(7)).is_ok());
    assert_eq!(mvm.read(&k, Timestamp(7)), None);
  }
}
