use crate::model::common::Timestamp;
use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

#[derive(Debug)]
pub struct MultiVersionMap<K, V> {
  pub map: HashMap<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>,
}

impl<K, V> MultiVersionMap<K, V>
where
  K: Eq + Hash + Clone,
  V: Clone,
{
  pub fn new() -> MultiVersionMap<K, V> {
    MultiVersionMap {
      map: HashMap::new(),
    }
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
      self
        .map
        .insert(key.clone(), (timestamp, vec![(timestamp, value)]));
      Ok(())
    }
  }

  pub fn read(&mut self, key: &K, timestamp: Timestamp) -> Option<V> {
    if let Some((lat, versions)) = self.map.get_mut(key) {
      *lat = max(*lat, timestamp);
      MultiVersionMap::<K, V>::find_version(versions, timestamp)
    } else {
      self.map.insert(key.clone(), (timestamp, vec![]));
      None
    }
  }

  /// Reads the version prior to the timestamp. This doesn't mutate
  /// the lat if the read happens with a future timestamp.
  pub fn static_read(&self, key: &K, timestamp: Timestamp) -> Option<V> {
    if let Some((_, versions)) = self.map.get(key) {
      MultiVersionMap::<K, V>::find_version(versions, timestamp)
    } else {
      None
    }
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
  use crate::storage::multiversion_map::MultiVersionMap;

  #[test]
  fn single_key_test() {
    let mut mvm = MultiVersionMap::new();
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
