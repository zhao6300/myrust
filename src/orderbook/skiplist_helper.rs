pub mod skiplist_serde {
    use serde::de::{Deserialize, Deserializer, MapAccess, Visitor};
    use serde::ser::{Serialize, SerializeMap, Serializer};
    use skiplist::SkipMap;
    use std::fmt;
    use std::marker::PhantomData;
    use std::ops::Neg;
    use std::result::Result;

    use super::super::{KeyOp, ValueOp};

    pub fn serialize<K, V, S>(skip_map: &SkipMap<K, V>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        K: Serialize + Ord + std::ops::Neg<Output = K> + Copy,
        V: Serialize + ValueOp,
    {
        let mut ser_map = serializer.serialize_map(Some(skip_map.len()))?;
        for (key, value) in skip_map.iter() {
            if value.get_reverse() {
                ser_map.serialize_key(&key.neg())?;
            } else {
                ser_map.serialize_key(key)?;
            }

            ser_map.serialize_value(&value)?;
        }
        ser_map.end()
    }

    pub fn deserialize<'de, K, V, D>(deserializer: D) -> Result<SkipMap<K, V>, D::Error>
    where
        D: Deserializer<'de>,
        K: Deserialize<'de> + Ord + std::ops::Neg<Output = K> + Copy,
        V: Deserialize<'de> + ValueOp,
    {
        deserializer.deserialize_map(SkipMapVisitor(PhantomData))
    }
    struct SkipMapVisitor<K, V>(PhantomData<fn() -> SkipMap<K, V>>);

    impl<'de, K, V> Visitor<'de> for SkipMapVisitor<K, V>
    where
        K: Deserialize<'de> + Ord + std::ops::Neg<Output = K> + Copy,
        V: Deserialize<'de> + ValueOp,
    {
        type Value = SkipMap<K, V>;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a skip map")
        }

        fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut skip_map: Self::Value = SkipMap::new();
            while let Some((key, value)) = map.next_entry::<K, V>()? {
                if value.get_reverse() {
                    skip_map.insert(key.neg(), value);
                } else {
                    skip_map.insert(key, value);
                }
            }
            Ok(skip_map)
        }
    }
}
