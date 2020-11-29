use crate::model::common::Timestamp;
use std::cmp::max;
use std::collections::HashMap;
use std::hash::Hash;

pub struct MultiVersionMap<K, V> {
    pub map: HashMap<K, (Timestamp, Vec<(Timestamp, Option<V>)>)>,
}

impl<K, V> MultiVersionMap<K, V>
where
    K: Eq + Hash,
{
    pub fn new() -> MultiVersionMap<K, V> {
        MultiVersionMap {
            map: HashMap::new(),
        }
    }

    pub fn write(&mut self, key: K, value: Option<V>, timestamp: Timestamp) {
        if let Some((lat, versions)) = self.map.get_mut(&key) {
            assert!(timestamp > *lat);
            *lat = timestamp;
            versions.push((timestamp, value));
        } else {
            self.map.insert(key, (timestamp, vec![(timestamp, value)]));
        }
    }

    pub fn read(&mut self, key: &K, timestamp: Timestamp) -> Option<&V> {
        if let Some((lat, versions)) = self.map.get_mut(key) {
            *lat = max(*lat, timestamp);
            MultiVersionMap::<K, V>::find_version(versions, timestamp)
        } else {
            None
        }
    }

    fn find_version(versions: &Vec<(Timestamp, Option<V>)>, timestamp: Timestamp) -> Option<&V> {
        for (t, value) in versions.iter().rev() {
            if let Some(v) = value {
                if *t <= timestamp {
                    return Some(v);
                }
            }
        }
        return None;
    }
}

#[cfg(test)]
mod tests {
    use crate::model::common::Timestamp;
    use crate::storage::multiversion_map::MultiVersionMap;
    use std::ops::Deref;

    #[test]
    fn test1() {
        let mut mvm = MultiVersionMap::new();
        let k = String::from("k");
        let v = String::from("v");
        mvm.write(k.clone(), Some(v.clone()), Timestamp(2));
        let ret = mvm.read(&k, Timestamp(2));
        assert_eq!(ret.unwrap().deref(), v);
    }
}
